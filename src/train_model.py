import os
import sys  # import sys for getting arguments from the command line call
import argparse  # import argparse for getting arguments from the command line
import yaml  # import yaml for pulling config file
from datetime import datetime  # import datetime for formatting of timestamps
import pandas as pd

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
 
configPath = os.path.join("config", "logging", "local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("train_model_log")

def pull_features(engine):
    """function for pulling features from a populated and updated database for training a model

    Given a database connection engine, access the database and pull the requested
    data as a Pandas dataframe.

    Args:
        engine (SQLAlchemy engine): the engine for working with a database

    Returns:
        features (pandas DataFrame): a dataframe containing the features columns for each event

    """
    logger.debug('Start of pull features function')

    features = pd.read_sql('SELECT * FROM features', engine)
    features['startDate'] = features['startDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())

    logger.debug('%s', features.head())

    return features


def train_models(model_type, features):
    """function for training a model of specified type using a set of passed features

    Args:
        model_type (str): an indicator of the type of model to train ('linear' or 'tree')
        features (Pandas DataFrame): a dataframe containing all the features data in the database

    Returns:
        classifier (Model object): the trained classification model
        regressor (Model object): the trained regression model

    """
    logger.debug('Start of train model function')

    # filter the data to only use past events and events that are already sold out
    training_data1 = features.loc[features['startDate'] < datetime.today()]
    training_data2 = features.loc[features['isSoldOut'] == 1]
    training_data = pd.merge(training_data1, training_data2, how='outer')
    logger.debug('Shape of training data: %s', training_data.shape)

    # drop the startDate (now that it's been used for filtering) and set the id to the index
    training_data = training_data.set_index('id').drop(columns=['startDate'])

    # pop off the classification and regression columns (isSoldOut and soldOutLead)
    y_class = training_data.pop('isSoldOut').values
    y_regress = training_data.pop('soldOutLead').values
    logger.debug('Shape of training classifications:, %s', y_class.shape)
    logger.debug('Shape of training regression values: %s', y_regress.shape)

    # identify the columns to use for numerical and categorical values
    all_columns = training_data.columns.values
    logger.debug('All columns: %s', all_columns)
    num_cols = ['minPrice', 'maxPrice', 'onSaleWindow', 'capacity']
    logger.debug('Numerical columns: %s', num_cols)
    cat_cols = [element for element in all_columns if element not in num_cols]
    logger.debug('Categorical columns: %s', cat_cols)

    # establish the numerical pipeline steps
    num_ss_step = ('ss', StandardScaler())
    num_steps = [num_ss_step]
    num_pipe = Pipeline(num_steps)

    # establish the categorical pipeline steps
    cat_ohe_step = ('ohe', OneHotEncoder(sparse=False, handle_unknown='ignore'))
    cat_steps = [cat_ohe_step]
    cat_pipe = Pipeline(cat_steps)

    # build the overall transformers
    transformers = [('cat', cat_pipe, cat_cols),
                    ('num', num_pipe, num_cols)]
    ct = ColumnTransformer(transformers=transformers)

    # if linear modeling is specified, then use a logistic regression for isSoldOut
    # and a linear regression for soldOutLead
    if model_type == 'linear':
        classifier = Pipeline([('transform', ct), ('log', LogisticRegression())])
        regressor = Pipeline([('transform', ct), ('regr', LinearRegression())])

    # if tree modeling is specified, the use decision trees for both types
    elif model_type == 'tree':
        classifier = Pipeline([('transform', ct), ('log', DecisionTreeClassifier())])
        regressor = Pipeline([('transform', ct), ('regr', DecisionTreeRegressor())])

    else:
        logger.error('Invalid/unsupport model type, should be "linear" or "tree"')
        sys.exit()

    # fit the models
    classifier.fit(training_data, y_class)
    regressor.fit(training_data, y_regress)

    # log the training errors and CV errors of the models
    logger.info('Classifier training accuracy: %s', classifier.score(training_data, y_class))
    logger.info('Regressor training r-squared: %s', regressor.score(training_data, y_regress))

    kf = KFold(n_splits=5, shuffle=True, random_state=123)
    logger.info('Classifier 5-fold CV score: %s', cross_val_score(classifier, training_data, y_class, cv=kf))
    logger.info('Regressor 5-fold CV score: %s', cross_val_score(regressor, training_data, y_regress, cv=kf))

    # return the models
    return classifier, regressor


def save_models(classifier, regressor, location):
    """function for saving fit models for later use

    Args:
        classifier (Model object): a trained classification model
        regressor (Model object): a trained regression model
        location (path): the path object for where to save the model (should be a directory)

    Returns:
        None

    """
    logger.debug('Start of save models function')

    class_file = os.path.join(location, 'classifier.pkl')
    regress_file = os.path.join(location, 'regressor.pkl')

    # save the trained model objects
    with open(class_file, "wb") as f:
        pickle.dump(classifier, f)
        logger.info("Trained classifier model object saved to %s", f.name)

    with open(regress_file, "wb") as f:
        pickle.dump(regressor, f)
        logger.info("Trained regressor model object saved to %s", f.name)


def run_train_model(args):
    """runs the model training scripts"""
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

    # pull the features
    features = pull_features(engine)

    # if a model_type argument was passed, then use it for calling the appropriate database type
    if args.model_type is not None:
        model_type = args.model_type

    # if no model_type argument was passed, then look for it in the config file
    elif "train_model" in config and "model_type" in config["train_model"]:
        model_type = config["train_model"]["model_type"]

    else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
        logger.error('Model type must be pass in arguments or in the config file')
        sys.exit()

    # train the models
    classifier, regressor = train_models(model_type, features)

    # save in models
    models_path = os.path.join('models')

    save_models(classifier, regressor, models_path)


if __name__ == '__main__':
    logger.debug('Start of train_model script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the type and location of the db
    parser = argparse.ArgumentParser(description="create database")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--type', default=None, help="type of database to create, 'sqlite' or 'mysql+pymysql'")
    parser.add_argument('--database_name', default=None,
                        help="location where database is to be created (including name.db)")
    parser.add_argument('--model_type', default='linear', help='type of models to train, should be "linear" or "tree"')

    args = parser.parse_args()

    # run the features generation based on the parsed arguments
    run_train_model(args)
