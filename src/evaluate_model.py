import os
import sys  # import sys for getting arguments from the command line call

sys.path.append(os.environ.get('PYTHONPATH'))
import argparse  # import argparse for getting arguments from the command line
import yaml  # import yaml for pulling config file
from datetime import datetime  # import datetime for formatting of timestamps
import logging.config  # import logging config

import pandas as pd
import boto3

configPath = os.path.join("config", "logging", "local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("evaluate_model_log")

from src.helpers.helpers import create_db_engine, \
    pull_features, pull_scores  # import helpers for creating an engine and pulling the features and scores tables


def evaluate_models(features, scores):
    """A function for evaluating the performance of models in predicting the results of event sell outs and lead time
    across prediction days. F1, Accuracy, Precision, and Correct Classification Rate (CCR) are computed and returned.

    Args:
        features (pandas DataFrame): a dataframe containing the features columns for each event
        scores (pandas DataFrame): a dataframe containing the scores columns for each event

    Returns:
        results (pandas DataFrame): a dataframe containing the summary statistics for predictions by day

    """
    logger.debug('Start of evaluate models function')

    ## pull the predictions info and compare it with actual results
    ## update accuracy/precision metrics

    # filter the data to only use past events or events that are already sold out
    stale_features1 = features#.loc[features['startDate'] < datetime.today()]
    stale_features2 = features.loc[features['isSoldOut'] == 1]
    stale_features = pd.merge(stale_features1, stale_features2, how='outer')
    logger.debug('Shape of old features data: %s', stale_features.shape)

    logger.debug('Shape of scores data: %s', scores.shape)

    # join the predictions with the features
    features_scores = pd.merge(stale_features, scores, how='inner', left_on='id', right_on='event_id')
    logger.debug('Shape of features merged with scores data: %s', features_scores.shape)

    # exit if no rows returned
    if features_scores.shape[0] == 0:
        logger.error('No predictions to compare to past events!')
        sys.exit()

    # convert the prediction date to a single date
    features_scores['predDate'] = features_scores['predictionDate'].apply(lambda x: datetime.date(x))

    # compute the status of each prediction (true positive, true negative, false positive, false negative)
    features_scores['result'] = features_scores.apply(
        lambda x: 'tp' if x['willSellOut'] == 1 and x['isSoldOut'] == 1 else (
        'fp' if x['willSellOut'] == 1 and x['isSoldOut'] == 0 else (
        'tn' if x['willSellOut'] == 0 and x['isSoldOut'] == 0 else (
        'fn' if x['willSellOut'] == 0 and x['isSoldOut'] == 1 else 'error'))), axis=1)
    features_scores['tp'] = features_scores['result'].apply(lambda x: 1 if x == 'tp' else None)
    features_scores['fp'] = features_scores['result'].apply(lambda x: 1 if x == 'fp' else None)
    features_scores['tn'] = features_scores['result'].apply(lambda x: 1 if x == 'tn' else None)
    features_scores['fn'] = features_scores['result'].apply(lambda x: 1 if x == 'fn' else None)

    # count the various factors per group
    fs_groups = features_scores.groupby(by='predDate')

    aggregations = {
        'tp': "count",
        'fp': "count",
        'tn': "count",
        'fn': "count"
    }

    results = fs_groups.agg(aggregations).copy()
    logger.debug('Shape of initial results: %s', results.shape)

    # compute the final statistics
    results['CCR'] = results.apply(
        lambda x: (x['tp'] + x['tn']) / (x['tp'] + x['tn'] + x['fp'] + x['fn']) if (x['tp'] + x['tn'] + x['fp'] + x['fn']) != 0 else 0, axis=1)
    results['Precision'] = results.apply(lambda x: (x['tp']) / (x['fp'] + x['tp']) if (x['fp'] + x['tp']) != 0 else 0,
                                         axis=1)
    results['Accuracy'] = results.apply(lambda x: (x['tp']) / (x['fn'] + x['tp']) if (x['fn'] + x['tp']) != 0 else 0,
                                        axis=1)
    results['F1'] = results.apply(lambda x: (2 * x['Precision'] * x['Accuracy']) / (x['Precision'] + x['Accuracy']) if (x['Precision'] + x['Accuracy']) != 0 else 0,
                                  axis=1)

    logger.debug('Shape of final results: %s', results.shape)

    return results


def save_results_local(results, location):
    """function for saving results to local

    Args:
        results(pandas DataFrame): a dataframe containing the summary statistics for evaluating a model
        location (path): the path object for where to save the results (should be a directory)

    Returns:
        None

    """
    logger.debug('Start of save results function')

    results_file = os.path.join(location, 'results.csv')

    # save the results
    with open(results_file, "w") as f:
        f.write(results.to_csv(index=True))
        logger.info("Results saved to %s", f.name)


def save_results_s3(results, location):
    """function for saving results to s3

    Args:
        results(pandas DataFrame): a dataframe containing the summary statistics for evaluating a model
        location (path): the path object for where to save the results (should be a directory)

    Returns:
        None

    """
    logging.info("Savings results to %s", location)

    # create an s3 resource
    s3 = boto3.resource('s3')

    try:  # try creating the object
        # build the results object
        results_name = 'results/results.csv'

        # create the s3 object
        results_file = s3.Object(location, results_name)

        # put the results into the body of the objects
        response = results_file.put(Body=results.to_csv(index=True))
        logger.info("Results uploaded as %s", response["ETag"])

    except Exception as e:
        logger.error(e)


def run_evaluate(args):
    """runs the model evalution scripts"""
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

    # pull the features and scores
    features = pull_features(engine)
    scores = pull_scores(engine)

    # create the results table
    results = evaluate_models(features, scores)

    # check for the specified save location type as an argument or in the config file
    if args.location_type is not None:
        save_type = args.location_type

    elif "evaluate_model" in config and "location_type" in config["evaluate_model"]:
        save_type = config["evaluate_model"]["location_type"]

    else:
        logger.error('location type must be pass in arguments or in the config file')
        sys.exit()

    # if the location type is 'local', then save the results locally
    if save_type == 'local':

        # if a save_location argument was passed, then use it
        if args.save_location is not None:
            save_location = args.save_location

        # if no save_location argument was passed, then look for it in the config file
        elif "evaluate_model" in config and "save_location" in config["evaluate_model"]:
            save_location = config["evaluate_model"]["save_location"]

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Save location must be passed in arguments or in the config file')
            sys.exit()

        # save in identified location
        save_path = os.path.join(save_location)

        save_results_local(results, save_path)

    # if the location type is 's3', then save the models in s3
    elif save_type == 's3':

        # if a save_location argument was passed, then use it
        if args.save_location is not None:
            save_location = args.save_location

        # if no save_location argument was passed, then look for it in the config file
        elif "evaluate_model" in config and "save_location" in config["evaluate_model"]:
            save_location = config["evaluate_model"]["save_location"]

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Save location must be passed in arguments or in the config file')
            sys.exit()

        # save in identified location
        save_path = os.path.join(save_location)

        save_results_s3(results, save_path)

    # otherwise, log the error and exit
    else:
        logger.error('location type must be "s3" or "local"')
        sys.exit()


if __name__ == '__main__':
    logger.debug('Start of evalute_model script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the type and location of the db and models
    parser = argparse.ArgumentParser(description="create database")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--type', default=None, help="type of database , 'sqlite' or 'mysql+pymysql'")
    parser.add_argument('--database_name', default=None,
                        help="location where database is (including name.db)")
    parser.add_argument('--save_location', default=None, help='location of where to save results')
    parser.add_argument('--location_type', default=None, help='whether the results will be saved locally or in s3')

    args = parser.parse_args()

    # run the evaluation based on the parsed arguments
    run_evaluate(args)


