import os
import sys  # import sys for getting arguments from the command line call
import argparse  # import argparse for getting arguments from the command line
import yaml  # import yaml for pulling config file
from datetime import datetime  # import datetime for formatting of timestamps
import pandas as pd
from sqlalchemy import Column, String, Integer, Boolean, DATETIME, DECIMAL  # import needed sqlalchemy libraries for db
from sqlalchemy.ext.declarative import declarative_base  # import for declaring classes
from sqlalchemy.orm import sessionmaker  # import the sessionmaker for adding data to the database
from sqlalchemy.ext.automap import automap_base # import for declaring classes

import numpy as np
from sklearn.preprocessing import *
from sklearn.linear_model import *
from sklearn.tree import *
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import KFold, cross_val_score

import pickle

import logging.config  # import logging config

from create_database import create_db_engine  # import a function for creating an engine
from train_model import pull_features  # import a function for pulling features

configPath = os.path.join("config", "logging", "local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("score_model_log")

## pull the features data for all events that have not yet occurred
## use the trained and pickled model from train_model.py to generate predictions
## save the predictions in a new table

def get_models(location):
    """function for opening loading saved models

    Args:
        location (path): the path object for where to save the model (should be a directory)

    Returns:
        classifier (Model object): a trained classification model
        regressor (Model object): a trained regression model

    """
    logger.debug('Start of get models function')

    class_file = os.path.join(location, 'classifier.pkl')
    regress_file = os.path.join(location, 'regressor.pkl')

    # save the trained model objects
    with open(class_file, "rb") as f:
        classifier = pickle.load(f)
        logger.info("Trained classifier model object loaded from %s", f.name)

    with open(regress_file, "rb") as f:
        regressor = pickle.load(f)
        logger.info("Trained regressor model object loaded from %s", f.name)

    return classifier, regressor


def score_models(classifier, regressor, features):
    """function for training a model of specified type using a set of passed features

    Args:
        classifier (Model object): the trained classification model
        regressor (Model object): the trained regression model
        features (Pandas DataFrame): a dataframe containing all the features data in the database

    Returns:
        scores (Pandas DataFrame): a dataframe containing the events, start dates, and their predictions (scores)

    """
    logger.debug('Start of score models function')

    # filter the data to only use future events and events that are not already sold out
    future_data1 = features.loc[features['startDate'] >= datetime.today()]
    future_data2 = features.loc[features['isSoldOut'] == 0]
    future_data = pd.merge(future_data1, future_data2, how='inner')
    logger.debug('Shape of future data: %s', future_data.shape)

    # score the models
    class_preds = classifier.predict(future_data)
    class_preds_probs = classifier.predict_proba(future_data)
    class_preds_confidence = np.amax(class_preds_probs, axis=1)
    regress_preds = regressor.predict(future_data)

    # build the scores table
    scores = future_data[['id','startDate']]
    scores['pred_id'] = scores['id'] + "-" + datetime.today().strftime('%y-%m-%d')
    scores['willSellOut'] = class_preds
    scores['confidence'] = class_preds_confidence
    scores['confidence'] = scores['confidence'].apply(lambda x: round(x, 2) * 100)
    scores['howFarOut'] = regress_preds
    scores['howFarOut'] = scores.apply(lambda x: 0 if x['howFarOut'] < 0 else (x['howFarOut'] if x['howFarOut'] <= (x['startDate'] - datetime.today()).days else ((x['startDate'] - datetime.today()).days if (x['startDate'] - datetime.today()).days >= 0 else 0)), axis=1)

    logger.debug('Shape of scores: %s', scores.shape)

    # return the scores
    return scores


def create_scores_table(engine):
    """function for creating a scores table in a database

    Given a database connection engine, access the database and create a scores table

    Args:
        engine (SQLAlchemy engine): the engine for working with a database

    Returns:
        None

    """
    # check if the scores table already exists, stop execution if it does
    if 'scores' in engine.table_names():
        logging.warning('scores table already exists!')

    else:
        logger.debug("Creating a scores table at %s", engine.url)

        Base = declarative_base()

        logger.debug("Creating the scores table")

        # create a score class
        class Score(Base):
            """Create a data model for the scores table """
            __tablename__ = 'scores'
            pred_id = Column(String(24), primary_key=True)
            event_id = Column(String(12), unique=False, nullable=False)
            startDate = Column(DATETIME(), unique=False, nullable=False)
            predictionDate = Column(DATETIME(), unique=False, nullable=False)
            willSellOut = Column(Boolean(), unique=False, nullable=False)
            confidence = Column(DECIMAL(), unique=False, nullable=False)
            howFarOut = Column(DECIMAL(), unique=False, nullable=False)

            def __repr__(self):
                return '<Score %r>' % self.id

        try:
            # create the tables
            Base.metadata.create_all(engine)

            # check that the tables were created
            for table in engine.table_names():
                logger.info("Created table %s", table)
        except Exception as e:
            logger.error("Could not create the database: %s", e)


def create_score(engine, score):
    """make a score to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	score (dict): dictionary for a score from the data source

    Returns:
    	score_to_add (Score): the built score to push to the database

    """
    logger.debug("Creating score %s to add to the database", score['pred_id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the score class from the scores table in the database
    Score = Base.classes.scores

    # build the score
    score_to_add = Score(pred_id=score['pred_id'],
        event_id=score['id'],
        startDate=datetime.fromisoformat(score['startDate']) if type(score['startDate']) == str else score['startDate'].to_pydatetime(),
        predictionDate=datetime.today(),
        willSellOut=score['willSellOut'],
        confidence=float(score['confidence']),
        howFarOut=score['howFarOut'])

    logger.debug("score %s was created", score_to_add.pred_id)

    # return the score
    return score_to_add


def update_score(engine, score):
    """update a score to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	score (dict): dictionary for an score from the data source

    Returns:
    	None

    """
    logger.debug('Update score %s', score['pred_id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the score class from the scores table in the database
    Score = Base.classes.scores

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # info changed flag
    new_info = False

    # query the score row
    score_row = session.query(Score).filter(Score.pred_id == score['pred_id']).first()

    # update the score details if necessary
    if score_row.willSellOut != score['willSellOut']:
        setattr(score_row, 'willSellOut', score['willSellOut'])
        new_info = True
        logging.debug('new willSellOut')

    if score_row.confidence != float(score['confidence']):
        setattr(score_row, 'confidence', float(score['confidence']))
        new_info = True
        logging.debug('new confidence')

    if score_row.howFarOut != score['howFarOut']:
        setattr(score_row, 'howFarOut', score['howFarOut'])
        new_info = True
        logging.debug('new howFarOut')

    # commit the data if a change was made
    if new_info:
        logger.debug('new score: %s: %s, %s, %s, %s', score_row.pred_id, score_row.startDate, score_row.willSellOut, score_row.confidence, score_row.howFarOut)
        session.commit()

    # close the session
    session.close()


def save_scores(engine, scores):
    """function for loading a scores dataset into a database

    Given a database connection engine, access the database and push the scores data into the scores table

    Args:
        engine (SQLAlchemy engine): the engine for working with a database
        scores (pandas DataFrame): a dataframe containing the scores columns for each event

    Returns:
        None

    """
    logger.info('Saving scores')

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the scores class from the scores table in the database
    Score = Base.classes.scores

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # initialize counters for the number of scores added and updated
    num_scores_added = 0
    num_scores_updated = 0

    # initialize a list of objects to add to the database
    objects_to_add = []

    # convert the dataframe into a dictionary of rows
    scores_dict = scores.to_dict('index')

    # query the scores table for all ids, creating a dictionary of keys (for fast lookup)
    scores_ids = session.query(Score.pred_id).all()
    scores_ids = {entity[0]: "" for entity in scores_ids}
    logger.debug('%s score ids retrieved', len(scores_ids))

    # pull the current scores dataframe
    current_scores = pd.read_sql('SELECT * FROM scores', engine)

    # for each score in the scores dict, check the score
    for score in scores_dict.values():
        # if the score id is in the current list, check against the pandas dataframe
        if score['pred_id'] in scores_ids.keys():
            # compare the score row to the corresponding row in the current scores dataframe from the database
            if list(current_scores[current_scores['pred_id'] == score['pred_id']].to_dict('index').values())[0] != score:
                # if the entries are different, then update the score
                update_score(engine, score)
                num_scores_updated += 1
            else:
                logger.debug('Score %s is the same', score['pred_id'])
        # otherwise, create the score and add it to the list of objects to add
        else:
            objects_to_add.append(create_score(engine, score))
            num_scores_added += 1

    # for each object to add, add it using the session
    num_added = 0
    for object in objects_to_add:
        session.add(object)
        logger.debug("Object %s added", object)
        num_added += 1

    session.commit()
    logger.info("%s objects added", num_added)

    logger.info("%s scores added, %s scores updated", num_scores_added, num_scores_updated)
    session.close()


def run_scoring(args):
    """runs the scoring script"""
    try:  # opens the specified config file
        with open(args.config, "r") as f:
            config = yaml.load(f, Loader=yaml.Loader)
    except Exception as e:
        logger.error('Error loading the config file: %s, be sure you specified a config.yml file', e)
        sys.exit()

    if config["database_info"]["how"] == "rds":
        # if a type argument was passed, then use it for calling the appropriate database type
        if args.type is not None:
            type = args.type

        # if no type argument was passed, then look for it in the config file
        elif "database_info" in config and "rds_database_type" in config["database_info"]:
            type = config["database_info"]["rds_database_type"]

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database type must be pass in arguments or in the config file')
            sys.exit()

        # if a database_name argument was passed, then use it for calling the appropriate database
        if args.database_name is not None:
            db_name = args.database_name

        # if no database_name argument was passed, then look for it in the config file
        elif "database_info" in config and "rds_database_name" in config["database_info"]:
            db_name = config["database_info"]["rds_database_name"]

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database name must be pass in arguments or in the config file')
            sys.exit()

        # create the engine for the database and type
        engine = create_db_engine(db_name, type)

    elif config["database_info"]["how"] == "local":
        # if a type argument was passed, then use it for calling the appropriate database type
        if args.type is not None:
            type = args.type

        # if no type argument was passed, then look for it in the config file
        elif "database_info" in config and "local_database_type" in config["database_info"]:
            type = config["database_info"]["local_database_type"]

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database type must be pass in arguments or in the config file')
            sys.exit()

        # if a database_name argument was passed, then use it for calling the appropriate database
        if args.database_name is not None:
            db_name = args.database_name

        # if no database_name argument was passed, then look for it in the config file
        elif "database_info" in config and "local_database_name" in config["database_info"]:
            db_name = os.path.join(config["database_info"]["local_database_folder"],
                                   config["database_info"]["local_database_name"])

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database name must be pass in arguments or in the config file')
            sys.exit()

        # create the engine for the database and type
        engine = create_db_engine(db_name, type)

    else:
        logger.error('Method of database storage (should be "rds" or "local") in config file not supported')
        sys.exit()

    # create the database schema in the engine
    create_scores_table(engine)

    # get the models
    models_path = os.path.join('models')
    classifier, regressor = get_models(models_path)

    # get the features
    features = pull_features(engine)

    # score the events
    scores = score_models(classifier, regressor, features)

    save_scores(engine, scores)


if __name__ == '__main__':
    logger.debug('Start of score_model Script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the type and location of the db
    parser = argparse.ArgumentParser(description="create database")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--type', default=None, help="type of database to create, 'sqlite' or 'mysql+pymysql'")
    parser.add_argument('--database_name', default=None,
                        help="location where database is to be created (including name.db)")

    args = parser.parse_args()

    # run the scoring based on the parsed arguments
    run_scoring(args)
