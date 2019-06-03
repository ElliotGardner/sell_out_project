"""Enables the command line execution of multiple modules within src/

This module combines the argparsing of each module within src/ and enables the execution of the corresponding scripts
so that all module imports can be absolute with respect to the main project directory.

To understand different arguments, run `python run.py --help`
"""
import argparse
import logging.config
from app.app import app

# Define LOGGING_CONFIG in config.py - path to config file for setting up the logger (e.g. config/logging/local.conf)
logging.config.fileConfig(app.config["LOGGING_CONFIG"])
logger = logging.getLogger("run-sell-out-project")
logger.debug('Starting run')

def run_app(args):
    app.run(debug=app.config["DEBUG"], port=app.config["PORT"], host=app.config["HOST"])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Run components of the model source code")
    subparsers = parser.add_subparsers()

    sb_run = subparsers.add_parser("app", description="Run Flask app")
    sb_run.set_defaults(func=run_app)

    args = parser.parse_args()
    args.func(args)
