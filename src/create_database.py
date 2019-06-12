import os  # import os for getting environment variables
import sys  # import sys for getting arguments from the command line call
sys.path.append(os.environ.get('PYTHONPATH'))
import argparse  # import for parsing arguments from the command line
import yaml  # import yaml for loading the config file
from datetime import datetime  # import datetime for formatting of timestamps
import logging.config  # import logging config

from sqlalchemy import Column, String, Integer, Boolean, DATETIME, DECIMAL  # import needed sqlalchemy libraries for db
from sqlalchemy.ext.declarative import declarative_base  # import for declaring classes

from src.helpers.helpers import create_db_engine  # helper function for creating a db engine

configPath = os.path.join("config","logging","local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("create_database_log")


def create_db(engine):
    """create a database at a specified location

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database

    Returns:
    	None

    """
    logger.debug("Creating a database at %s", engine.url)

    Base = declarative_base()

    logger.debug("Creating the events table")
    class Event(Base):
        """Create a data model for the events table """
        __tablename__ = 'events'
        id = Column(String(12), primary_key=True)
        name = Column(String(255), unique=False, nullable=False)
        startDate = Column(DATETIME(), unique=False, nullable=False)
        endDate = Column(DATETIME(), unique=False, nullable=True)
        publishedDate = Column(DATETIME(), unique=False, nullable=True)
        onSaleDate = Column(DATETIME(), unique=False, nullable=True)
        venueId = Column(Integer(), unique=False, nullable=False)
        categoryId = Column(Integer(), unique=False, nullable=False)
        formatId = Column(Integer(), unique=False, nullable=False)
        inventoryType = Column(String(30), unique=False, nullable=True)
        isFree = Column(Boolean(), unique=False, nullable=True, default=False)
        isReservedSeating = Column(Boolean(), unique=False, nullable=True, default=False)
        isAvailable = Column(Boolean(), unique=False, nullable=True, default=True)
        isSoldOut = Column(Boolean(), unique=False, nullable=True, default=False)
        soldOutDate = Column(DATETIME(), unique=False, nullable=True, default=datetime(2019, 4, 12, 0, 0, 1))
        hasWaitList = Column(Boolean(), unique=False, nullable=True, default=False)
        minPrice = Column(DECIMAL(), unique=False, nullable=True, default=0.00)
        maxPrice = Column(DECIMAL(), unique=False, nullable=True, default=0.00)
        capacity = Column(Integer(), unique=False, nullable=True, default=10000)
        ageRestriction = Column(String(30), unique=False, nullable=True, default='none')
        doorTime = Column(String(30), unique=False, nullable=True)
        presentedBy = Column(String(255), unique=False, nullable=True)
        isOnline = Column(Boolean(), unique=False, nullable=True, default = False)
        url = Column(String(255), unique=False, nullable=False)
        lastInfoDate = Column(DATETIME(), unique=False, nullable=True)


        def __repr__(self):
            return '<Event %r>' % self.id


    logger.debug("Creating the venues table")
    class Venue(Base):
        """Create a data model for the venues table """
        __tablename__ = 'venues'
        id = Column(Integer(), primary_key=True)
        name = Column(String(255), unique=False, nullable=True)
        city = Column(String(40), unique=False, nullable=True)
        ageRestriction = Column(String(30), unique=False, nullable=True)
        capacity = Column(Integer, unique=False, nullable=True, default=10000)

        def __repr__(self):
            return '<Venue %r>' % self.id


    logger.debug("Creating the formats table")
    class Frmat(Base):
        """Create a data model for the formats table """
        __tablename__ = 'frmats'
        id = Column(Integer(), primary_key=True)
        name = Column(String(30), unique=False, nullable=False)

        def __repr__(self):
            return '<Format %r>' % self.id


    logger.debug("Creating the categories table")
    class Category(Base):
        """Create a data model for the categories table """
        __tablename__ = 'categories'
        id = Column(Integer(), primary_key=True)
        name = Column(String(30), unique=False, nullable=False)

        def __repr__(self):
            return '<Category %r>' % self.id

    try:
        # create the tables
        Base.metadata.create_all(engine)

        # check that the tables were created
        for table in engine.table_names():
            logger.info("Created table %s", table)
    except Exception as e:
        logger.error("Could not create the database: %s", e)


def run_create(args):
    """runs the creation script"""
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

        # create the database schema in the engine
        create_db(engine)

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
            db_name = os.path.join(config["database_info"]["local_database_folder"],config["database_info"]["local_database_name"])

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database name must be pass in arguments or in the config file')
            sys.exit()

        # create the engine for the database and type
        engine = create_db_engine(db_name, type)

        # create the database schema in the engine
        create_db(engine)

    else:
        logger.error('Method of database storage (should be "rds" or "local") in config file not supported')
        sys.exit()

if __name__ == '__main__':
    logger.debug('Start of create_database Script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the type and location of the db
    parser = argparse.ArgumentParser(description="create database")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--type', default=None, help="type of database to create, 'sqlite' or 'mysql+pymysql'")
    parser.add_argument('--database_name', default=None, help="location where database is to be created (including name.db)")

    args = parser.parse_args()

    # run the creation based on the parsed arguments
    run_create(args)