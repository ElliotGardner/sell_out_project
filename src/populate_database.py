import os
import sys  # import sys for getting arguments from the command line call
import argparse  # import argparse for getting arguments from the command line
import yaml  # import yaml for pulling config file
import json  # import json for reading JSON files from the s3 raw data bucket
from datetime import datetime  # import datetime for formatting of timestamps
import logging.config  # import logging config

import boto3  # import boto3 for access s3
from sqlalchemy.orm import sessionmaker  # import the sessionmaker for adding data to the database
from sqlalchemy.ext.automap import automap_base # import for declaring classes

from src.helpers.helpers import API_request, set_headers, create_db_engine  # import helper functions for API requests, headers setting, and creating a DB engine
from src.helpers.helpers import create_event, create_venue, create_frmat, create_category  # import helper functions for DB creation

configPath = os.path.join("config","logging","local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("populate_database_log")


def initial_populate_format_categories(engine, frmats_URL, categories_URL, headers=None):
    """a function for putting an initial set of formats and categories into an empty database

    This function should only be called when starting with an empty database, rather than filling
    a database that already has data. Within this function, calls to the database will be made in
    order to check for currently populated results. If a partial fill occurs, then running this function
    will only fill in the blank tables. This initial pull occurs using an API call to the current
    data in the Eventbrite API, rather than using the landed JSONs which represent historical data.

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	frmats_URL (str): the URL for the API call to get formats
    	categories_URL (str): the URL for the API call to get categories
    	headers (dict): the headers to use for the API call

    Returns:
    	None

    """
    logger.debug('Start of initial populate format and categories to database function')

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the classes from the tables in the database
    Frmat = Base.classes.frmats
    Category = Base.classes.categories

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # initialize a list of objects to add to the database
    objects_to_add = []


    # query the formats table, counting the number of current entries (should be 0)
    num_frmats = session.query(Frmat).count()
    logger.debug('Number of existing formats: %s', num_frmats)

    # if the formats table has no results, then make an API call to get them
    if num_frmats == 0:
        logger.info('Retrieving formats...')
        # get the formats results from the API
        frmats = API_request(frmats_URL, headers=headers)

        # for each format returned, create a Format for the database and add it to the list of objects to add
        for frmat in frmats['formats']:
            objects_to_add.append(create_frmat(engine, frmat))
            num_frmats += 1

        logger.info("%s formats pulled", num_frmats)


    # query the categories table, counting the number of current entries (should be 0)
    num_categories = session.query(Category).count()
    logger.debug('Number of existing categories: %s', num_categories)

    # if the categories table has no results, then make an API call to get them
    if num_categories == 0:
        logger.info('Retrieving subcategories...')
        # get the categories results from the API
        page = 1
        categories = API_request(categories_URL, headers=headers)

        # for each category returned, create an Category for the database and add it to the list of objects to add
        for category in categories['subcategories']:
            objects_to_add.append(create_category(engine, category))
            num_categories += 1

        # check if more categories exist, and pull those if they do
        logger.debug('has_more_items is %s', categories['pagination']['has_more_items'])
        while categories['pagination']['has_more_items']:
            # update the page number to request and pull the new data
            page += 1
            logger.debug('Retrieving Page %s...', page)
            categories = API_request(categories_URL, page_num=page, headers=headers)

            # for each category returned, create an Category for the database and add it to the list of objects to add
            for category in categories['subcategories']:
                objects_to_add.append(create_category(engine, category))
                num_categories += 1

            logger.debug('has_more_items is %s', categories['pagination']['has_more_items'])

        logger.info("%s pages received", page)
        logger.info("%s subcategories pulled", num_categories)

    # for each object to add, add it using the session
    num_added = 0
    for object in objects_to_add:
        session.add(object)
        logger.debug("Object %s added", object.id)
        num_added += 1

    session.commit()
    logger.info("%s objects added", num_added)
    session.close()


def initial_populate_events_venues(engine, raw_data_location, location_type):
    """a function for putting an initial set of events and venues into an empty database

    This function should only be called when starting with an empty database, rather than filling
    a database that already has data. Within this function, calls to the database will be made in
    order to check for currently populated results. If a partial fill occurs, then running this function
    will only fill in the blank tables. This initial pull occurs using raw data stored in a specified
    location, which should be the landed JSONs from the ingest_data script

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	raw_data_location (str): the location of where the raw events and venues data resides
    	location_type (str): a flag for the type of location, should be 'local' or 's3'

    Returns:
    	None

    """
    logger.debug('Start of initial populate events and venues to database function')

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the classes from the tables in the database
    Event = Base.classes.events
    Venue = Base.classes.venues

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # initialize a list of objects to add to the database
    objects_to_add = []

    # query the formats table, counting the number of current entries (should be 0)
    num_events = session.query(Event).count()
    logger.debug('Number of existing events: %s', num_events)

    # query the categories table, counting the number of current entries (should be 0)
    venues_query = session.query(Venue)
    num_venues = venues_query.count()
    all_venues = venues_query.all()
    venue_ids = []
    for venue in all_venues:
        venue_ids.append(venue.id)
    logger.debug('Number of existing venues: %s', num_venues)

    event_ids = []

    # if the formats table has no results, then make an API call to get them
    if num_events == 0:
        logger.info('Retrieving events...')
        # get the events results from the location
        if location_type == 's3':
            # load the bucket
            s3 = boto3.resource("s3")
            bucket = s3.Bucket(raw_data_location)

            # get all the objects in the bucket
            all_objects = list(bucket.objects.all())

            # build a sorting key function for parsing the date from a filename
            def parseDate(object):
                try:
                    date = object.key[4:-5]
                    date = datetime.strptime(date, '%Y/%m/%d/%H_%M_%S_%f')
                except Exception as e:
                    date = datetime.today()
                return date

            # sort the object list so that it is in order of pull date
            all_objects.sort(key=parseDate)

            # for each object in the bucket, query it, load the body
            for object in all_objects:
                logging.debug('Parsing JSON %s', object.key)
                response = object.get()
                body = response['Body'].read()

                # filter out empty response
                if str(body) != "b''":
                    # load the output of the body as a dictionary
                    output = json.loads(body)

                    # for each event in the events list of the output, create an event and venue and add to the list of objects to add
                    for event in output['events']:
                        # if the event id isn't in the current list, add it
                        if event['id'] not in event_ids:
                            objects_to_add.append(create_event(engine, event, output['PullTime']))
                            event_ids.append(event['id'])
                            num_events += 1

                        # if the venue_id isn't in the current list, add it
                        if event['venue_id'] not in venue_ids:
                            objects_to_add.append(create_venue(engine, event))
                            venue_ids.append(event['venue_id'])
                            num_venues += 1


        logger.info("%s events pulled", num_events)
        logger.info("%s venues pulled", num_venues)

    # for each object to add, add it using the session
    num_added = 0
    for object in objects_to_add:
        session.add(object)
        logger.debug("Object %s added", object)
        num_added += 1

    session.commit()
    logger.info("%s objects added", num_added)
    session.close()


def run_populate(args):
    """runs the population script"""
    try:  # opens the specified config file
        with open(args.config, "r") as f:
            config = yaml.load(f, Loader=yaml.Loader)
    except Exception as e:
        logger.error('Error loading the config file: %s, be sure you specified a config.yml file', e)
        sys.exit()

    # if an API_token argument was passed, then use it for creating the headers
    if args.API_token is not None:
        headers = set_headers(args.API_token)

    # if no API_token argument was passed, then look for it in the config file
    elif "ingest_data" in config and "API_token" in config["ingest_data"]:
        headers = set_headers(config["ingest_data"]["API_token"])

    else:  # if no additional arguments were passed and the config file didn't have it, then prompt for a token
        headers = set_headers()
    logger.debug('headers: %s', headers)


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

        # if no database_name argument was passed, then look for it in the config file
        if "populate_database" in config and "initial_populate_format_categories" in config["populate_database"]:
            # run the initial population of the formats and categories
            initial_populate_format_categories(engine, headers=headers, **config['populate_database']['initial_populate_format_categories'])

        else:  # if the config file didn't have the right entries, then log the error and exit
            logger.error('initial_populate_format_categories must be passed in the config file')
            sys.exit()

        if "populate_database" in config and "initial_populate_events_venues" in config["populate_database"]:
            # run the initial population of the formats and categories
            initial_populate_events_venues(engine, **config['populate_database']['initial_populate_events_venues'])

        else:  # if the config file didn't have the right entries, then log the error and exit
            logger.error('initial_populate_event_venues must be passed in the config file')
            sys.exit()


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


        if "populate_database" in config and "initial_populate_format_categories" in config["populate_database"]:
            # run the initial population of the formats and categories
            initial_populate_format_categories(engine, headers=headers, **config['populate_database']['initial_populate_format_categories'])

        else:  # if the config file didn't have the right entries, then log the error and exit
            logger.error('initial_populate_format_categories must be passed in the config file')
            sys.exit()


        if "populate_database" in config and "initial_populate_events_venues" in config["populate_database"]:
            # run the initial population of the formats and categories
            initial_populate_events_venues(engine, **config['populate_database']['initial_populate_events_venues'])

        else:  # if the config file didn't have the right entries, then log the error and exit
            logger.error('initial_populate_event_venues must be passed in the config file')
            sys.exit()


    else:
        logger.error('Method of database storage (should be "rds" or "local") in config file not supported')
        sys.exit()


if __name__ == '__main__':
    logger.debug('Start of populate_database Script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the type and location of the db
    parser = argparse.ArgumentParser(description="populate database")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--type', default=None, help="type of database to populate")
    parser.add_argument('--database_name', default=None, help="location where database to populate is located (including name.db)")
    parser.add_argument('--API_token', default=None, help="API OAuth Token for API calls")

    args = parser.parse_args()

    # run the population based on the parsed arguments
    run_populate(args)
