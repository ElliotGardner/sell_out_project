import os  # import os for getting environment variables
import sys  # import sys for getting arguments from the command line call
import argparse  # import for parsing arguments from the command line
import yaml  # import yaml for loading the config file
from datetime import datetime  # import datetime for formatting of timestamps
from sqlalchemy import create_engine, Column, String, Integer, Boolean, DATETIME, DECIMAL, JSON  # import needed sqlalchemy libraries for db
from sqlalchemy.ext.declarative import declarative_base  # import for declaring classes
import logging.config  # import logging config

logging.config.fileConfig("config\\logging\\local.conf")
logger = logging.getLogger("create_database_log")


def create_db(database_name, type):
    """create a database at a specified location

    Args:
    	database_name (str): the name of the database to create
    	type (str): the type of database to create

    Returns:
    	None

    """
    logger.debug("Creating a %s database at %s", type, database_name)

    Base = declarative_base()


    logger.debug("Creating the events table")
    class Event(Base):
        """Create a data model for the events table """
        __tablename__ = 'events'
        id = Column(Integer(), primary_key=True)
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
        allData = Column(JSON(), unique=False, nullable=False)

        def __repr__(self):
            return '<Event %r>' % self.id


    logger.debug("Creating the venues table")
    class Venue(Base):
        """Create a data model for the venues table """
        __tablename__ = 'venues'
        id = Column(Integer(), primary_key=True)
        name = Column(String(255), unique=False, nullable=False)
        city = Column(String(40), unique=False, nullable=True)
        ageRestriction = Column(String(30), unique=False, nullable=True)
        capacity = Column(Integer, unique=False, nullable=True, default=10000)

        def __repr__(self):
            return '<Venue %r>' % self.id


    logger.debug("Creating the formats table")
    class Format(Base):
        """Create a data model for the formats table """
        __tablename__ = 'formats'
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
        if type == "sqlite":
            # set up sqlite connection
            engine_string = type + ":///" + database_name

        if type == "mysql+pymysql":
            # set up mysql connection
            # the engine_string format
            # engine_string = "{conn_type}:///{user}:{password}@{host}:{port}/{database}"
            conn_type = type
            user = os.environ.get("MYSQL_USER")
            password = os.environ.get("MYSQL_PASSWORD")
            host = os.environ.get("MYSQL_HOST")
            port = os.environ.get("MYSQL_PORT")
            engine_string = "{}:///{}:{}@{}:{}/{}".format(conn_type, user, password, host, port, database_name)

        # create the engine
        engine = create_engine(engine_string)

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

    if config["create_database"]["how"] == "rds":
        # if a type argument was passed, then use it for calling the appropriate database type
        if args.type is not None:
            type = args.type

        # if no type argument was passed, then look for it in the config file
        elif "create_database" in config and "rds_database_type" in config["create_database"]:
            type = config["create_database"]["rds_database_type"]

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database type must be pass in arguments or in the config file')
            sys.exit()

        # if a database_name argument was passed, then use it for calling the appropriate database
        if args.database_name is not None:
            db_name = args.database_name

        # if no database_name argument was passed, then look for it in the config file
        elif "create_database" in config and "rds_database_name" in config["create_database"]:
            db_name = config["create_database"]["rds_database_name"]

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database name must be pass in arguments or in the config file')
            sys.exit()

        create_db(db_name, type)

    if config["create_database"]["how"] == "local":
        # if a type argument was passed, then use it for calling the appropriate database type
        if args.type is not None:
            type = args.type

        # if no type argument was passed, then look for it in the config file
        elif "create_database" in config and "local_database_type" in config["create_database"]:
            type = config["create_database"]["local_database_type"]

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database type must be pass in arguments or in the config file')
            sys.exit()

        # if a database_name argument was passed, then use it for calling the appropriate database
        if args.database_name is not None:
            db_name = args.database_name

        # if no database_name argument was passed, then look for it in the config file
        elif "create_database" in config and "local_database_name" in config["create_database"]:
            db_name = os.path.join(config["create_database"]["local_database_folder"],config["create_database"]["local_database_name"])

        else:  # if no additional arguments were passed and the config file didn't have it, then log the error and exit
            logger.error('Database name must be pass in arguments or in the config file')
            sys.exit()

        create_db(db_name, type)

if __name__ == '__main__':
    logger.debug('Start of create_database Script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the type and location of the db
    parser = argparse.ArgumentParser(description="create database")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--type', default=None, help="type of database to create")
    parser.add_argument('--database_name', default=None, help="location where database is to be created (including name.db)")

    args = parser.parse_args()

    # run the creation based on the parsed arguments
    run_create(args)