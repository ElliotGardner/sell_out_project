"""Enables the command line execution of multiple modules within src/

This module combines the argparsing of each module within src/ and enables the execution of the corresponding scripts
so that all module imports can be absolute with respect to the main project directory.

To understand different arguments, run `python run.py --help`
"""
import os
import sys
sys.path.append(os.environ.get('PYTHONPATH'))
import argparse
import logging.config

# Define LOGGING_CONFIG in config.py - path to config file for setting up the logger (e.g. config/logging/local.conf)
configPath = os.path.join("config", "logging", "local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("run-sell-out-project")
logger.debug('Starting run')

from app.app import app
from src.ingest_data import run_ingest
from src.create_database import run_create
from src.populate_database import run_populate
from src.update_database import run_update
from src.generate_features import run_generate
from src.train_model import run_train_model
from src.score_model import run_scoring
from src.evaluate_model import run_evaluate

def run_app(args):
    app.run(debug=app.config["DEBUG"], port=app.config["PORT"], host=app.config["HOST"])

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Run components of the model source code")
    subparsers = parser.add_subparsers()

    sb_ingest = subparsers.add_parser("ingest", description="Ingest data from the API")
    sb_ingest.add_argument("--config", default=None, help="Location of configuration yaml")
    sb_ingest.add_argument("--API_token", default=None, help="API token for eventbrite pull")
    sb_ingest.set_defaults(func=run_ingest)

    sb_create = subparsers.add_parser("create", description="Create the database for storing records")
    sb_create.add_argument("--config", default=None, help="Location of configuration yaml")
    sb_create.add_argument('--type', default=None, help="type of database to create, 'sqlite' or 'mysql+pymysql'")
    sb_create.add_argument('--database_name', default=None,
                        help="location where database is to be created (including name.db)")
    sb_create.set_defaults(func=run_create)

    sb_populate = subparsers.add_parser("populate", description="Populate the database with an initial set of records")
    sb_populate.add_argument("--config", default=None, help="Location of configuration yaml")
    sb_populate.add_argument('--type', default=None, help="type of database, 'sqlite' or 'mysql+pymysql'")
    sb_populate.add_argument('--database_name', default=None,
                           help="location where database to populate is located (including name.db)")
    sb_populate.add_argument('--API_token', default=None, help="API OAuth Token for API calls")
    sb_populate.set_defaults(func=run_populate)

    sb_update = subparsers.add_parser("update", description="Update the database with a new set of records")
    sb_update.add_argument("--config", default=None, help="Location of configuration yaml")
    sb_update.add_argument('--type', default=None, help="type of database, 'sqlite' or 'mysql+pymysql'")
    sb_update.add_argument('--database_name', default=None,
                             help="location where database to update is located (including name.db)")
    sb_update.add_argument('--API_token', default=None, help="API OAuth Token for API calls")
    sb_update.add_argument('--formats_cats', default=False, help="Whether to update formats and categories or not")
    sb_update.set_defaults(func=run_update)

    sb_features = subparsers.add_parser("features", description="Generate features for the set of events in the db")
    sb_features.add_argument("--config", default=None, help="Location of configuration yaml")
    sb_features.add_argument('--type', default=None, help="type of database, 'sqlite' or 'mysql+pymysql'")
    sb_features.add_argument('--database_name', default=None,
                           help="location of the database (including name.db)")
    sb_features.set_defaults(func=run_generate)

    sb_train = subparsers.add_parser("train", description="Train models based on the features")
    sb_train.add_argument("--config", default=None, help="Location of configuration yaml")
    sb_train.add_argument('--type', default=None, help="type of database, 'sqlite' or 'mysql+pymysql'")
    sb_train.add_argument('--database_name', default=None,
                             help="location of the database (including name.db)")
    sb_train.add_argument('--model_type', default='linear', help='type of models to train, should be "linear" or "tree"')
    sb_train.add_argument('--model_location', default=None, help='location of where to save models')
    sb_train.add_argument('--location_type', default=None, help='whether the models will be saved locally or in s3')
    sb_train.set_defaults(func=run_train_model)

    sb_score = subparsers.add_parser("score", description="Score the models based on the features")
    sb_score.add_argument("--config", default=None, help="Location of configuration yaml")
    sb_score.add_argument('--type', default=None, help="type of database, 'sqlite' or 'mysql+pymysql'")
    sb_score.add_argument('--database_name', default=None,
                          help="location of the database (including name.db)")
    sb_score.add_argument('--model_location', default=None, help='location of where the models were saved')
    sb_score.add_argument('--location_type', default=None, help='whether the models were saved locally or in s3')
    sb_score.set_defaults(func=run_scoring)

    sb_evaluate = subparsers.add_parser("evaluate", description="Evaluate the models based on the actual data vs predictions")
    sb_evaluate.add_argument("--config", default=None, help="Location of configuration yaml")
    sb_evaluate.add_argument('--type', default=None, help="type of database, 'sqlite' or 'mysql+pymysql'")
    sb_evaluate.add_argument('--database_name', default=None,
                          help="location of the database (including name.db)")
    sb_evaluate.add_argument('--save_location', default=None, help='location of where the results should be saved')
    sb_evaluate.add_argument('--location_type', default=None, help='whether the results should be saved locally or in s3')
    sb_evaluate.set_defaults(func=run_evaluate)

    flask_run = subparsers.add_parser("app", description="Run Flask app")
    flask_run.set_defaults(func=run_app)

    args = parser.parse_args()
    args.func(args)
