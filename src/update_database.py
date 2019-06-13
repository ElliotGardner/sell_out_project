import os
import sys  # import sys for getting arguments from the command line call
sys.path.append(os.environ.get('PYTHONPATH'))
import argparse  # import argparse for getting arguments from the command line
import yaml  # import yaml for pulling config file
import json  # import json for reading JSON files from the s3 raw data bucket
import re
import logging.config  # import logging config

from datetime import datetime  # import datetime for formatting of timestamps
from sqlalchemy.orm import sessionmaker  # import the sessionmaker for adding data to the database
from sqlalchemy.ext.automap import automap_base # import for declaring classes
import boto3  # import boto3 for access s3
import pandas as pd

configPath = os.path.join("config","logging","local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("update_database_log")

from src.helpers.helpers import API_request, set_headers, create_db_engine  # import helper functions for API requests, headers setting, and creating a DB engine
from src.helpers.helpers import create_event, create_venue, create_frmat, create_category  # import helper functions for DB creation
from src.helpers.helpers import update_event, update_venue, update_frmat, update_category  # import helper functions for DB update
from src.helpers.helpers import event_to_event_dict, event_to_venue_dict  # import helpers for event and venue comparison as dicts


def update_format_categories(engine, frmats_URL, categories_URL, headers=None):
    """a function for updating a set of formats and categories into a database

    This function should only be called for a filled database. Within this function, calls to the database will be
    made in order to check for currently populated results. The new data pull occurs using an API call to the current
    data in the Eventbrite API.

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	frmats_URL (str): the URL for the API call to get formats
    	categories_URL (str): the URL for the API call to get categories
    	headers (dict): the headers to use for the API call

    Returns:
    	None

    """
    logger.debug('Start of update format and categories to database function')

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

    # initialize counters for the number of formats updated and added
    num_added = 0
    num_updated = 0

    # query the formats table for all ids, creating a dictionary of keys (for fast lookup)
    frmat_ids = session.query(Frmat.id).all()
    frmat_ids = {entity[0]:"" for entity in frmat_ids}
    logger.debug('%s frmat ids retrieved', len(frmat_ids))

    # make an API call to get the new formats list
    logger.info('Retrieving formats...')
    frmats = API_request(frmats_URL, headers=headers)

    # for each format returned, check against the current list
    for frmat in frmats['formats']:
        # if the format is already in the database, update it
        if int(frmat['id']) in frmat_ids:
            update_frmat(engine, frmat)
            num_updated += 1
        # otherwise, create the format and add it to the object to add
        else:
            objects_to_add.append(create_frmat(engine, frmat))
            num_added += 1

    logger.info("%s formats added, %s formats updated", num_added, num_updated)

    # reset the counters for the number of categories updated and added to 0
    num_added = 0
    num_updated = 0

    # query the categories table for all ids, creating a dictionary of keys (for fast lookup)
    category_ids = session.query(Category.id).all()
    category_ids = {entity[0]: "" for entity in category_ids}
    logger.debug('%s category ids retrieved', len(category_ids))

    # make an API call to get the new categories list
    logger.info('Retrieving subcategories...')
    # get the categories results from the API
    page = 1
    categories = API_request(categories_URL, headers=headers)

    # for each category returned, check against the current list
    for category in categories['subcategories']:
        # if the category is already in the database, update it
        if int(category['id']) in category_ids:
            update_category(engine, category)
            num_updated += 1
        # otherwise, create the category and add it to the object to add
        else:
            objects_to_add.append(create_category(engine, category))
            num_added += 1

    # check if more categories exist, and pull those if they do
    logger.debug('has_more_items is %s', categories['pagination']['has_more_items'])
    while categories['pagination']['has_more_items']:
        # update the page number to request and pull the new data
        page += 1
        logger.debug('Retrieving Page %s...', page)
        categories = API_request(categories_URL, page_num=page, headers=headers)

        # for each category returned, check against the current list
        for category in categories['subcategories']:
            # if the category is already in the database, update it
            if int(category['id']) in category_ids:
                update_category(engine, category)
                num_updated += 1
            # otherwise, create the category and add it to the object to add
            else:
                objects_to_add.append(create_category(engine, category))
                num_added += 1

        logger.debug('has_more_items is %s', categories['pagination']['has_more_items'])

    logger.info("%s pages received", page)
    logger.info("%s categories added, %s categories updated", num_added, num_updated)

    # for each object to add, add it using the session
    num_added = 0
    for object in objects_to_add:
        session.add(object)
        logger.debug("Object %s added", object.id)
        num_added += 1

    session.commit()
    logger.info("%s objects added", num_added)
    session.close()


def update_events_venues(engine, raw_data_location, location_type):
    """a function for upating the set of events and venues in a populated database

    This function should only be called when starting with a populated database. Within this function,
    calls to the database will be made in order to check for currently populated results. This function
    uses raw data stored in a specified location, which should be the landed JSONs from the ingest_data
    script. The information examined is only that which is newer than the "last update" stored in a config
    file called "last_update.txt". As the updates are committed, the last_update date is overwritten, so
    if the script is stopped early, then the old data will not need to be re-examined. If a refresh is
    needed, then reset the "last_update.txt" file to reflect a date (in YY-MM-DD-HH-MM-SS format) before
    the first data pull, in this case "19-01-01-01-01-01" will work.

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	raw_data_location (str): the location of where the raw events and venues data resides
    	location_type (str): a flag for the type of location, should be 'local' or 's3'

    Returns:
    	None

    """
    logger.debug('Start of update events and venues in database function')

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

    # initialize counters for the number of events and venues updated and added
    num_events_added = 0
    num_events_updated = 0
    num_venues_added = 0
    num_venues_updated = 0
    overall_objects_added = 0

    # pull the last update date
    update_path = os.path.join('config','last_update.txt')
    try:
        with open(update_path, mode='r') as f:
            last_update_date = f.readline()
            logger.debug('last update date: %s', last_update_date)
    except Exception as e:
        last_update_date = '90-01-01-01-01-01'

    # initialize a tracker for the latest date of data
    new_update_date = datetime.strptime(last_update_date, '%y-%m-%d-%H-%M-%S')
    current_update_date = datetime.strptime(last_update_date, '%y-%m-%d-%H-%M-%S')

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

        # for each object in the bucket, check against the last update date, then query it, load the body if new
        for object in all_objects:
            object_date = parseDate(object)
            logging.debug('Parsing JSON %s', object.key)

            # if the object date is older than the current_update_date, then don't read it
            if object_date < current_update_date:
                logging.debug('Old Data')
            else:
                # if the parsed date is greater than the new_update_date, then replace it
                if object_date > new_update_date:
                    new_update_date = object_date

                # read the body of the object
                response = object.get()
                body = response['Body'].read()

                # filter out empty response
                if str(body) != "b''" and object.key[-5:] == '.json':
                    # load the output of the body as a dictionary
                    output = json.loads(body)

                    # initialize a list of objects to add to the database
                    objects_to_add = []

                    # initialize a list of venue ids added to prevent attempting to add the same venue multiple times
                    new_venue_ids = []

                    # query the events and venues tables for all ids, creating a dictionary of keys (for fast lookup)
                    event_ids = session.query(Event.id).all()
                    event_ids = {entity[0]: "" for entity in event_ids}
                    logger.debug('%s event ids retrieved', len(event_ids))

                    # pull the current events dataframe
                    current_events = pd.read_sql('SELECT * FROM events', engine)
                    current_events = current_events.drop(columns=['lastInfoDate', 'soldOutDate'])
                    current_events['startDate'] = current_events['startDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())
                    current_events['endDate'] = current_events['endDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())
                    current_events['publishedDate'] = current_events['publishedDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())
                    current_events['onSaleDate'] = current_events['onSaleDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())

                    venue_ids = session.query(Venue.id).all()
                    venue_ids = {entity[0]: "" for entity in venue_ids}
                    logger.debug('%s venue ids retrieved', len(venue_ids))

                    # pull the current venues dataframe
                    current_venues = pd.read_sql('SELECT * FROM venues', engine)

                    # for each event in the events list of the output, check the event and venue
                    for event in output['events']:
                        # if the event id is in the current list, update it
                        if event['id'] in event_ids.keys():
                            event_dict = event_to_event_dict(event)

                            # compare the event row to the corresponding row in the current events dataframe from the database
                            if list(current_events[current_events['id'] == event['id']].to_dict(
                                    'index').values())[0] != event_dict:
                                # if the entries are different, then update the feature
                                update_event(engine, event, output['PullTime'])
                                num_events_updated += 1
                            else:
                                logger.debug('Event %s is the same', event['id'])
                        # otherwise, create the event and add it to the list of objects to add
                        else:
                            objects_to_add.append(create_event(engine, event, output['PullTime']))
                            num_events_added += 1

                        # if the venue_id is in the current list, update it
                        if int(event['venue_id']) in venue_ids.keys():
                            venue_dict = event_to_venue_dict(event)

                            # compare the venue row to the corresponding row in the current venues dataframe from the database
                            if list(current_venues[current_venues['id'] == int(event['venue_id'])].to_dict('index').values())[0] != venue_dict:
                                # if the entries are different, then update the feature
                                update_venue(engine, event)
                                num_venues_updated += 1
                            else:
                                logger.debug('Venue %s is the same', event['venue_id'])
                        # otherwise, create the venue and add it to the list of objects to add
                        elif int(event['venue_id']) not in new_venue_ids:
                            objects_to_add.append(create_venue(engine, event))
                            num_venues_added += 1
                            new_venue_ids.append(int(event['venue_id']))
                        # otherwise log that the event is already set to be added
                        else:
                            logging.debug('Venue %s already set to be added', event['venue_id'])

                    # for each object to add, add it using the session
                    num_added = 0
                    for object in objects_to_add:
                        session.add(object)
                        logger.debug("Object %s added", object)
                        num_added += 1

                    session.commit()
                    logger.info("%s objects added", num_added)
                    overall_objects_added += num_added

                    # update the last_update_date
                    update_path = os.path.join('config', 'last_update.txt')
                    new_update_date_txt = datetime.strftime(new_update_date, '%y-%m-%d-%H-%M-%S')
                    logging.info('New latest update is %s', new_update_date_txt)
                    with open(update_path, mode='w') as f:
                        f.write(new_update_date_txt)

    if location_type == 'local':
        # load the files
        folder = raw_data_location

        all_objects = []

        for parent, directory, files in os.walk(os.path.join(os.getcwd(), folder)):
            all_objects = all_objects + [os.path.join(parent, file) for file in files if file[-5:] == '.json']

            # build a sorting key function for parsing the date from a filename

        def parseDate(object):
            try:
                # get the info after the "raw" folder part of the path
                date_name = re.split(r'raw', object)[1][1:-5]
                full_date, time = os.path.split(date_name)
                year_month, day = os.path.split(full_date)
                year, month = os.path.split(year_month)
                time_split = re.split('_', time)
                hour = time_split[0]
                minute = time_split[1]
                second = time_split[2]
                date = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))

            except Exception as e:
                date = datetime.today()

            return date

        # sort the object list so that it is in order of pull date
        all_objects.sort(key=parseDate)

        # for each object in the bucket, check against the last update date, then query it, load the body if new
        for object in all_objects:
            object_date = parseDate(object)
            logging.debug('Parsing JSON %s', object)

            # if the object date is older than the current_update_date, then don't read it
            if object_date < current_update_date:
                logging.debug('Old Data')
            else:
                # if the parsed date is greater than the new_update_date, then replace it
                if object_date > new_update_date:
                    new_update_date = object_date

                with open(object, 'r') as f:
                    body = f.read()

                    # load the output of the body as a dictionary
                    output = json.loads(body)

                    # initialize a list of objects to add to the database
                    objects_to_add = []

                    # initialize a list of venue ids added to prevent attempting to add the same venue multiple times
                    new_venue_ids = []

                    # query the events and venues tables for all ids, creating a dictionary of keys (for fast lookup)
                    event_ids = session.query(Event.id).all()
                    event_ids = {entity[0]: "" for entity in event_ids}
                    logger.debug('%s event ids retrieved', len(event_ids))

                    # pull the current events dataframe
                    current_events = pd.read_sql('SELECT * FROM events', engine)
                    current_events = current_events.drop(columns=['lastInfoDate', 'soldOutDate'])
                    current_events['startDate'] = current_events['startDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())
                    current_events['endDate'] = current_events['endDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())
                    current_events['publishedDate'] = current_events['publishedDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())
                    current_events['onSaleDate'] = current_events['onSaleDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())

                    venue_ids = session.query(Venue.id).all()
                    venue_ids = {entity[0]: "" for entity in venue_ids}
                    logger.debug('%s venue ids retrieved', len(venue_ids))

                    # pull the current venues dataframe
                    current_venues = pd.read_sql('SELECT * FROM venues', engine)

                    # for each event in the events list of the output, check the event and venue
                    for event in output['events']:
                        # if the event id is in the current list, update it
                        if event['id'] in event_ids.keys():
                            event_dict = event_to_event_dict(event)

                            # compare the event row to the corresponding row in the current events dataframe from the database
                            if list(current_events[current_events['id'] == event['id']].to_dict(
                                    'index').values())[0] != event_dict:
                                # if the entries are different, then update the feature
                                update_event(engine, event, output['PullTime'])
                                num_events_updated += 1
                            else:
                                logger.debug('Event %s is the same', event['id'])
                        # otherwise, create the event and add it to the list of objects to add
                        else:
                            objects_to_add.append(create_event(engine, event, output['PullTime']))
                            num_events_added += 1

                        # if the venue_id is in the current list, update it
                        if int(event['venue_id']) in venue_ids.keys():
                            venue_dict = event_to_venue_dict(event)

                            # compare the venue row to the corresponding row in the current venues dataframe from the database
                            if list(current_venues[current_venues['id'] == int(event['venue_id'])].to_dict('index').values())[0] != venue_dict:
                                # if the entries are different, then update the feature
                                update_venue(engine, event)
                                num_venues_updated += 1
                            else:
                                logger.debug('Venue %s is the same', event['venue_id'])
                        # otherwise, create the venue and add it to the list of objects to add
                        elif int(event['venue_id']) not in new_venue_ids:
                            objects_to_add.append(create_venue(engine, event))
                            num_venues_added += 1
                            new_venue_ids.append(int(event['venue_id']))
                        # otherwise log that the event is already set to be added
                        else:
                            logging.debug('Venue %s already set to be added', event['venue_id'])

                    # for each object to add, add it using the session
                    num_added = 0
                    for obj in objects_to_add:
                        session.add(obj)
                        logger.debug("Object %s added", obj)
                        num_added += 1

                    session.commit()
                    logger.info("%s objects added", num_added)
                    overall_objects_added += num_added

                    # update the last_update_date
                    update_path = os.path.join('config', 'last_update.txt')
                    new_update_date_txt = datetime.strftime(new_update_date, '%y-%m-%d-%H-%M-%S')
                    logging.info('New latest update is %s', new_update_date_txt)
                    with open(update_path, mode='w') as f:
                        f.write(new_update_date_txt)

    logger.info("%s events added, %s events updated", num_events_added, num_events_updated)
    logger.info("%s venues added, %s venues updated", num_venues_added, num_venues_updated)

    session.commit()
    logger.info("%s objects added", overall_objects_added)
    session.close()


def run_update(args):
    """runs the update script"""
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

        # if the args passed specified an update to formats and categories, then conduct it
        if args.formats_cats:
            if "update_database" in config and "update_format_categories" in config["update_database"]:
                # run the update of the formats and categories
                update_format_categories(engine, headers=headers,
                                         **config['update_database']['update_format_categories'])

            else:  # if the config file didn't have the right entries, then log the error and exit
                logger.error('update_format_categories must be passed in the config file')
                sys.exit()

        if "update_database" in config and "update_events_venues" in config["update_database"]:
            # run the update of the events and venues
            update_events_venues(engine, **config['update_database']['update_events_venues'])

        else:  # if the config file didn't have the right entries, then log the error and exit
            logger.error('update_event_venues must be passed in the config file')
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

        # if the args passed specified an update to formats and categories, then conduct it
        if args.formats_cats:
            if "update_database" in config and "update_format_categories" in config["update_database"]:
                # run the update of the formats and categories
                update_format_categories(engine, headers=headers, **config['update_database']['update_format_categories'])

            else:  # if the config file didn't have the right entries, then log the error and exit
                logger.error('update_format_categories must be passed in the config file')
                sys.exit()

        if "update_database" in config and "update_events_venues" in config["update_database"]:
            # run the update of the events and venues
            update_events_venues(engine, **config['update_database']['update_events_venues'])

        else:  # if the config file didn't have the right entries, then log the error and exit
            logger.error('update_event_venues must be passed in the config file')
            sys.exit()

    else:
        logger.error('Method of database storage (should be "rds" or "local") in config file not supported')
        sys.exit()


if __name__ == '__main__':
    logger.debug('Start of update_database Script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the type and location of the db
    parser = argparse.ArgumentParser(description="update database")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--type', default=None, help="type of database to update")
    parser.add_argument('--database_name', default=None, help="location where database to update is located (including name.db)")
    parser.add_argument('--API_token', default=None, help="API OAuth Token for API calls")
    parser.add_argument('--formats_cats', default=False, help="Whether to update formats and categories or not")

    args = parser.parse_args()

    # run the update based on the parsed arguments
    run_update(args)
