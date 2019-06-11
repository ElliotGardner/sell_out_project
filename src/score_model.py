import os
import sys  # import sys for getting arguments from the command line call
sys.path.append(os.environ.get('PYTHONPATH'))
import argparse  # import argparse for getting arguments from the command line
import yaml  # import yaml for pulling config file
from datetime import datetime  # import datetime for formatting of timestamps
import pickle
import logging.config  # import logging config

import pandas as pd
from sqlalchemy.orm import sessionmaker  # import the sessionmaker for adding data to the database
from sqlalchemy.ext.automap import automap_base # import for declaring classes
from sqlalchemy import Column, String, Integer, Boolean, DATETIME, DECIMAL  # import needed sqlalchemy libraries for db
from sqlalchemy.ext.declarative import declarative_base  # import for declaring classes
import numpy as np
import boto3

from src.helpers.helpers import create_db_engine, pull_features  # import helpers for creating an engine and pulling features
from src.helpers.helpers import create_score, update_score  # import helpers for creating and updating scores

configPath = os.path.join("config", "logging", "local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("score_model_log")


def get_models_local(location):
    """function for opening loading saved models from a local folder

    Args:
        location (path): the path object for where the models were saved (should be a directory)

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


def get_models_s3(location):
    """function for opening loading saved models from s3

    Args:
        location (path): the bucket for where the models were saved

    Returns:
        classifier (Model object): a trained classification model
        regressor (Model object): a trained regression model

    """
    logger.debug('Start of get models function')

    # load the bucket
    s3 = boto3.resource("s3")

    # build the model object names
    fullname_class = 'models/classifier.pkl'
    fullname_regress = 'models/regressor.pkl'

    # create the s3 objects
    obj_class = s3.Object(location, fullname_class)
    obj_regress = s3.Object(location, fullname_regress)

    response1 = obj_class.get()
    body1 = response1['Body'].read()
    classifier = pickle.loads(body1)
    logger.info("Trained classifier model object loaded from %s", obj_class.key)

    response2 = obj_regress.get()
    body2 = response2['Body'].read()
    regressor = pickle.loads(body2)
    logger.info("Trained regressor model object loaded from %s", obj_regress.key)

    return classifier, regressor


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
            logger.error('Database type must be passed in arguments or in the config file')
            sys.exit()

        # if a database_name argument was passed, then use it for calling the appropriate database
        if args.database_name is not None:
            db_name = args.database_name

        # if no database_name argument was passed, then look for it in the config file
        elif "database_info" in config and "rds_database_name" in config["database_info"]:
            db_name = config["database_info"]["rds_database_name"]

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database name must be passed in arguments or in the config file')
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
            logger.error('Database type must be passed in arguments or in the config file')
            sys.exit()

        # if a database_name argument was passed, then use it for calling the appropriate database
        if args.database_name is not None:
            db_name = args.database_name

        # if no database_name argument was passed, then look for it in the config file
        elif "database_info" in config and "local_database_name" in config["database_info"]:
            db_name = os.path.join(config["database_info"]["local_database_folder"],
                                   config["database_info"]["local_database_name"])

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database name must be passed in arguments or in the config file')
            sys.exit()

        # create the engine for the database and type
        engine = create_db_engine(db_name, type)

    else:
        logger.error('Method of database storage (should be "rds" or "local") in config file not supported')
        sys.exit()

    # create the database schema in the engine
    create_scores_table(engine)

    # if a model_location argument was passed, then use it
    if args.model_location is not None:
        model_location = args.model_location

    # if no model_location argument was passed, then look for it in the config file
    elif "model_info" in config and "model_location" in config["model_info"]:
        model_location = config["model_info"]["model_location"]

    else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
        logger.error('Model location must be passed in arguments or in the config file')
        sys.exit()

    # check for the specified save location type as an argument or in the config file
    if args.location_type is not None:
        save_type = args.location_type

    elif "model_info" in config and "location_type" in config["model_info"]:
        save_type = config["model_info"]["location_type"]

    else:
        logger.error('location type must be pass in arguments or in the config file')
        sys.exit()

    # get the models
    if save_type == 'local':
        models_path = os.path.join(model_location)
        classifier, regressor = get_models_local(models_path)

    elif save_type == 's3':
        classifier, regressor = get_models_s3(model_location)

    else:
        logger.error('location type must be pass in arguments or in the config file as "s3" or "local"')
        sys.exit()

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
    parser.add_argument('--model_location', default=None, help='location of where to save models')
    parser.add_argument('--location_type', default=None, help='whether the models will be saved locally or in s3')


    args = parser.parse_args()

    # run the scoring based on the parsed arguments
    run_scoring(args)
