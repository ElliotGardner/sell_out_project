import os
import sys  # import sys for getting arguments from the command line call
import argparse  # import argparse for getting arguments from the command line
import yaml  # import yaml for pulling config file
from datetime import datetime  # import datetime for formatting of timestamps
import logging.config  # import logging config

import pandas as pd
import re
from sqlalchemy.orm import sessionmaker  # import the sessionmaker for adding data to the database
from sqlalchemy.ext.automap import automap_base # import for declaring classes
from sqlalchemy.ext.declarative import declarative_base  # import for declaring classes
from sqlalchemy import Column, String, Integer, Boolean, DATETIME, DECIMAL  # import needed sqlalchemy libraries for db

configPath = os.path.join("config", "logging", "local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("generate_features_log")

from src.helpers.helpers import create_db_engine, create_feature, update_feature  # import helpers for creating an engine, creating and updating features

def convert_data_to_features(engine):
    """function for pulling data from a populated and updated database for training a model

    Given a database connection engine, access the database and pull the requested
    data as a Pandas dataframe.

    Args:
        engine (SQLAlchemy engine): the engine for working with a database

    Returns:
        features (pandas DataFrame): a dataframe containing the features columns for each event

    """
    logger.debug('Start of pull and convert data to features function')

    events = pd.read_sql('SELECT * FROM events', engine)

    logger.debug('%s', events.head())

    venues = pd.read_sql('SELECT * FROM venues', engine)

    logger.debug('%s', venues.head())

    data = pd.merge(events, venues, how='left', left_on='venueId', right_on='id', suffixes=('_event', '_venue'))

    logger.debug('%s', data.head())

    data['onSaleWindow'] = (
        data['startDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime()) - data['onSaleDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())).apply(
        lambda x: x.days)

    data['eventWeekday'] = data['startDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime()).apply(lambda x: x.weekday())

    data['startHour'] = data['startDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime()).apply(lambda x: x.hour)

    data['capacity'] = data.apply(lambda x: x['capacity_event'] if x['capacity_event'] <= x['capacity_venue'] and x[
                                                                                                                      'capacity_event'] != 10000 else
    x['capacity_venue'], axis=1)

    data['city'] = data['city'].apply(lambda x: "" if x is None else x).apply(str.lower).apply(str.strip)

    data['locale'] = data['city'].apply(lambda x: x if x == 'chicago' else 'suburbs')

    data['ageRestriction'] = data.apply(
        lambda x: x['ageRestriction_event'] if x['ageRestriction_event'] is not None else x['ageRestriction_venue'] if
        x['ageRestriction_venue'] is not None else "None", axis=1)

    data['presentedBy_simple'] = data['presentedBy'].apply(lambda x: (
        "Harmonica Dunn" if re.search('harmonica dunn', x.lower()) is not None else (
            "Elbo Room" if re.search('elbo room', x.lower()) is not None else "Other")) if x is not None else "Other")

    data['soldOutLead'] = data.apply(
        lambda x: max(((datetime.fromisoformat(x['startDate']) if type(x['startDate']) == str else x['startDate'].to_pydatetime()) - (datetime.fromisoformat(x['soldOutDate']) if type(x['soldOutDate']) == str else x['soldOutDate'].to_pydatetime())).days, 0) if x[
                                                                                                                          'isSoldOut'] == 1 else 0,
        axis=1)

    data['venueName'] = data['name_venue'].apply(lambda x: x.lower() if x is not None else "unknown")

    venueCounts = data['venueName'].value_counts()

    venueCounts1 = venueCounts[venueCounts >= 10]

    mainVenues = venueCounts1.index

    data['venueName_simple'] = data['venueName'].apply(lambda x: x if x in mainVenues else "other")

    data['minPrice'] = data['minPrice'].fillna(0)

    data['maxPrice'] = data['maxPrice'].fillna(0)

    data = data.rename(columns={'id_event': 'id'})

    features = data[
        ['id', 'startDate', 'categoryId', 'formatId', 'inventoryType', 'isFree', 'isReservedSeating', 'minPrice', 'maxPrice',
         'venueName_simple', 'onSaleWindow', 'eventWeekday', 'startHour', 'capacity', 'locale', 'ageRestriction',
         'presentedBy_simple', 'isSoldOut', 'soldOutLead']]

    logger.debug('%s', features.head())

    return features


def create_features_table(engine):
    """function for creating a features table in a database

    Given a database connection engine, access the database and create a features table

    Args:
        engine (SQLAlchemy engine): the engine for working with a database

    Returns:
        None

    """
    # check if the features table already exists, stop execution if it does
    if 'features' in engine.table_names():
        logging.warning('features table already exists!')

    else:
        logger.debug("Creating a features table at %s", engine.url)

        Base = declarative_base()

        logger.debug("Creating the features table")

        # create a feature class
        class Feature(Base):
            """Create a data model for the events table """
            __tablename__ = 'features'
            id = Column(String(12), primary_key=True)
            startDate = Column(DATETIME(), unique=False, nullable=False)
            categoryId = Column(Integer(), unique=False, nullable=False)
            formatId = Column(Integer(), unique=False, nullable=False)
            inventoryType = Column(String(30), unique=False, nullable=False)
            isFree = Column(Boolean(), unique=False, nullable=False)
            isReservedSeating = Column(Boolean(), unique=False, nullable=False)
            minPrice = Column(DECIMAL(), unique=False, nullable=False)
            maxPrice = Column(DECIMAL(), unique=False, nullable=False)
            venueName_simple = Column(String(255), unique=False, nullable=False)
            onSaleWindow = Column(Integer(), unique=False, nullable=False)
            eventWeekday = Column(Integer(), unique=False, nullable=False)
            startHour = Column(Integer(), unique=False, nullable=False)
            capacity = Column(Integer(), unique=False, nullable=False)
            locale = Column(String(10), unique=False, nullable=False)
            ageRestriction = Column(String(30), unique=False, nullable=False)
            presentedBy_simple = Column(String(30), unique=False, nullable=False)
            isSoldOut = Column(Boolean(), unique=False, nullable=False)
            soldOutLead = Column(Integer(), unique=False, nullable=False)

            def __repr__(self):
                return '<Feature %r>' % self.id

        try:
            # create the tables
            Base.metadata.create_all(engine)

            # check that the tables were created
            for table in engine.table_names():
                logger.info("Created table %s", table)
        except Exception as e:
            logger.error("Could not create the database: %s", e)


def save_features(engine, features):
    """function for loading a features dataset into a database

    Given a database connection engine, access the database and push the features data into the features table

    Args:
        engine (SQLAlchemy engine): the engine for working with a database
        features (pandas DataFrame): a dataframe containing the features columns for each event

    Returns:
        None

    """
    logger.info('Saving features')

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the feature class from the features table in the database
    Feature = Base.classes.features

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # initialize counters for the number of features updated and added
    num_features_added = 0
    num_features_updated = 0

    # initialize a list of objects to add to the database
    objects_to_add = []

    # convert the dataframe into a dictionary of rows
    features_dict = features.to_dict('index')

    # query the features table for all ids, creating a dictionary of keys (for fast lookup)
    feature_ids = session.query(Feature.id).all()
    feature_ids = {entity[0]: "" for entity in feature_ids}
    logger.debug('%s feature ids retrieved', len(feature_ids))

    # pull the current features dataframe
    current_features = pd.read_sql('SELECT * FROM features', engine)

    # for each feature in the features dict, check the feature
    for feature in features_dict.values():
        # if the feature id is in the current list, check against the pandas dataframe
        if feature['id'] in feature_ids.keys():
            # compare the feature row to the corresponding row in the current features dataframe from the database
            if list(current_features[current_features['id'] == feature['id']].to_dict('index').values())[0] != feature:
                # if the entries are different, then update the feature
                update_feature(engine, feature)
                num_features_updated += 1
            else:
                logger.debug('Feature %s is the same', feature['id'])
        # otherwise, create the feature and add it to the list of objects to add
        else:
            objects_to_add.append(create_feature(engine, feature))
            num_features_added += 1

    # for each object to add, add it using the session
    num_added = 0
    for object in objects_to_add:
        session.add(object)
        logger.debug("Object %s added", object)
        num_added += 1

    session.commit()
    logger.info("%s objects added", num_added)

    logger.info("%s features added, %s features updated", num_features_added, num_features_updated)
    session.close()


def run_generate(args):
    """runs the feature generation script"""
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
        create_features_table(engine)

        # pull the data and make a features dataframe
        features = convert_data_to_features(engine)

        # save the features into the database
        save_features(engine, features)

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

        # create the database schema in the engine
        create_features_table(engine)

        # pull the data and make a features dataframe
        features = convert_data_to_features(engine)

        # save the features into the database
        save_features(engine, features)

    else:
        logger.error('Method of database storage (should be "rds" or "local") in config file not supported')
        sys.exit()


if __name__ == '__main__':
    logger.debug('Start of generate_features Script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the type and location of the db
    parser = argparse.ArgumentParser(description="create features")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--type', default=None, help="type of database, 'sqlite' or 'mysql+pymysql'")
    parser.add_argument('--database_name', default=None,
                        help="location of database (including name.db)")

    args = parser.parse_args()

    # run the generation based on the parsed arguments
    run_generate(args)
