import os
import sys  # import sys for getting arguments from the command line call
import argparse  # import argparse for getting arguments from the command line
import yaml  # import yaml for pulling config file
import json  # import json for reading JSON files from the s3 raw data bucket
import boto3  # import boto3 for access s3
from datetime import datetime  # import datetime for formatting of timestamps
from sqlalchemy.orm import sessionmaker  # import the sessionmaker for adding data to the database
from sqlalchemy.ext.automap import automap_base # import for declaring classes
import logging.config  # import logging config

from create_database import create_db_engine  # import a function for creating an engine
from populate_database import create_event, create_venue, create_frmat, create_category  # import for creating new formats, categories, venues, and events
from ingest_data import API_request, set_headers  # import for initially populating events, venues, formats, and categories

configPath = os.path.join("config","logging","local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("update_database_log")

def update_event(engine, event, infoDate):
    """update an event to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	event (dict): dictionary for an event from the data source
    	infoDate (str): the date the info is from (pull date)

    Returns:
    	None

    """
    logger.debug('Update event %s', event['id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the Event class from the events table in the database
    Event = Base.classes.events

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # info changed flag
    new_info = False

    # newly sold out flag
    newly_sold_out = False

    # no longer sold out flag
    no_longer_sold_out = False

    # query the event row
    event_row = session.query(Event).filter(Event.id == event['id']).first()

    # if the infoDate of the passed event dictionary is before the last info date for the event (shouldn't happen
    # with sorted data), just check the sold out dates
    if datetime.strptime(infoDate, '%y-%m-%d-%H-%M-%S') < event_row.lastInfoDate:
        # if the event is listed as sold out in both the database and the passed event dictionary, and the soldOutDate
        # is greater than the infoDate of this pull, then change the sold out date to the earlier info date
        if (event['ticket_availability']['is_sold_out']) and (event_row.isSoldOut) and (event_row.soldOutDate > datetime.strptime(infoDate, '%y-%m-%d-%H-%M-%S')):
            setattr(event_row, 'soldOutDate', datetime.strptime(infoDate, '%y-%m-%d-%H-%M-%S'))
            new_info = True
            logging.debug('new sold out date from earlier info')

    # otherwise the infoDate should be the same or newer than the last info date, so check for updates
    else:
        # update the event details if necessary
        if event_row.name != event['name']['text']:
            setattr(event_row, 'name', event['name']['text'])
            new_info = True
            logging.debug('new name')

        if event_row.startDate != datetime.fromisoformat(event['start']['local']):
            setattr(event_row, 'startDate', datetime.fromisoformat(event['start']['local']))
            new_info = True
            logging.debug('new start date')

        if event_row.endDate != datetime.fromisoformat(event['end']['local']):
            setattr(event_row, 'endDate', datetime.fromisoformat(event['end']['local']))
            new_info = True
            logging.debug('new end date')

        if event_row.publishedDate != datetime.fromisoformat(event['published'][0:19]):
            setattr(event_row, 'publishedDate', datetime.fromisoformat(event['published'][0:19]))
            new_info = True
            logging.debug('new published date')

        if (event['ticket_availability']['start_sales_date'] is not None) and (event_row.onSaleDate != datetime.fromisoformat(event['ticket_availability']['start_sales_date']['local'])):
            setattr(event_row, 'onSaleDate', datetime.fromisoformat(event['ticket_availability']['start_sales_date']['local']))
            new_info = True
            logging.debug('new on sale date')

        if (event['subcategory_id'] is not None) and (event_row.categoryId != int(event['subcategory_id'])):
            setattr(event_row, 'categoryId', int(event['subcategory_id']))
            new_info = True
            logging.debug('new category')

        if (event['format_id'] is not None) and (event_row.formatId != int(event['format_id'])):
            setattr(event_row, 'formatId', int(event['format_id']))
            new_info = True
            logging.debug('new format id')

        if event_row.inventoryType != event['inventory_type']:
            setattr(event_row, 'inventoryType', event['inventory_type'])
            new_info = True
            logging.debug('new inventory type')

        if event_row.isReservedSeating != event['is_reserved_seating']:
            setattr(event_row, 'isReservedSeating', event['is_reserved_seating'])
            new_info = True
            logging.debug('new isReservedSeating')

        if event_row.isAvailable != event['ticket_availability']['has_available_tickets']:
            setattr(event_row, 'isAvailable', event['ticket_availability']['has_available_tickets'])
            new_info = True
            logging.debug('new isAvailable')

        if event_row.isSoldOut != event['ticket_availability']['is_sold_out']:
            # if the event wasn't sold out, but now is, then change the "newly sold out" flag
            if (not event_row.isSoldOut) and (event['ticket_availability']['is_sold_out']):
                newly_sold_out = True

            # if the event was sold out, but now isn't, then change the "no longer sold out" flag
            if (event_row.isSoldOut) and (not event['ticket_availability']['is_sold_out']):
                no_longer_sold_out = True

            setattr(event_row, 'isSoldOut', event['ticket_availability']['is_sold_out'])
            new_info = True
            logging.debug('new isSoldOut')

        # if the event is newly sold out, then change the sold out date to the info date
        if newly_sold_out:
            setattr(event_row, 'soldOutDate', datetime.strptime(infoDate, '%y-%m-%d-%H-%M-%S'))
            new_info = True
            logging.debug('new sold out date')

        # if the event is no longer sold out, then change the sold out date to the default option
        if no_longer_sold_out:
            setattr(event_row, 'soldOutDate', datetime(2019,4,12,0,0,1))
            new_info = True
            logging.debug('cleared sold out date')

        if event_row.hasWaitList != event['ticket_availability']['waitlist_available']:
            setattr(event_row, 'hasWaitList', event['ticket_availability']['waitlist_available'])
            new_info = True
            logging.debug('new hasWaitList')

        if (event['ticket_availability']['minimum_ticket_price'] is not None) and (event_row.minPrice != float(event['ticket_availability']['minimum_ticket_price']['major_value'])):
            setattr(event_row, 'minPrice', float(event['ticket_availability']['minimum_ticket_price']['major_value']))
            new_info = True
            logging.debug('new min price')

        if (event['ticket_availability']['maximum_ticket_price'] is not None) and (event_row.maxPrice != float(event['ticket_availability']['maximum_ticket_price']['major_value'])):
            setattr(event_row, 'maxPrice', float(event['ticket_availability']['maximum_ticket_price']['major_value']))
            new_info = True
            logging.debug('new max price')

        if (event['capacity'] is not None) and (event_row.capacity != int(event['capacity'])):
            setattr(event_row, 'capacity', int(event['capacity']))
            new_info = True
            logging.debug('new capacity')

        if event_row.ageRestriction != event['music_properties']['age_restriction']:
            setattr(event_row, 'ageRestriction', event['music_properties']['age_restriction'])
            new_info = True
            logging.debug('new age restriction')

        if event_row.doorTime != event['music_properties']['door_time']:
            setattr(event_row, 'doorTime', event['music_properties']['door_time'])
            new_info = True
            logging.debug('new door time')

        if event_row.presentedBy != event['music_properties']['presented_by']:
            setattr(event_row, 'presentedBy', event['music_properties']['presented_by'])
            new_info = True
            logging.debug('new presented by')

        if event_row.isOnline != event['online_event']:
            setattr(event_row, 'isOnline', event['online_event'])
            new_info = True
            logging.debug('new isOnline')

        # change the "last info date" to the info date of the checked event
        setattr(event_row, 'lastInfoDate', datetime.strptime(infoDate, '%y-%m-%d-%H-%M-%S'))


    # commit the data if a change was made
    if new_info:
        logger.debug('new event: %s: %s, %s, %s', event_row.id, event_row.name, event_row.isSoldOut, event_row.soldOutDate)
        session.commit()

    # close the session
    session.close()


def update_venue(engine, event):
    """update a venue to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	event (dict): dictionary for an event (which contains a venue) from the data source

    Returns:
    	None

    """
    logger.debug('Update venue %s', event['venue_id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the Venue class from the venues table in the database
    Venue = Base.classes.venues

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # info changed flag
    new_info = False

    # query the venue row
    venue_row = session.query(Venue).filter(Venue.id == event['venue_id']).first()

    if event['venue'] is not None:
    # update the venue details if necessary
        if venue_row.name != event['venue']['name']:
            setattr(venue_row, 'name', event['venue']['name'])
            new_info = True
            logging.debug('new name')

        if venue_row.city != event['venue']['address']['city']:
            setattr(venue_row, 'city', event['venue']['address']['city'])
            new_info = True
            logging.debug('new city')

        if event['venue']['capacity'] is not None:
            if venue_row.capacity != int(event['venue']['capacity']):
                setattr(venue_row, 'capacity', int(event['venue']['capacity']))
                new_info = True
                logging.debug('new capacity')

        if venue_row.ageRestriction != event['venue']['age_restriction']:
            setattr(venue_row, 'ageRestriction', event['venue']['age_restriction'])
            new_info = True
            logging.debug('new age restriction')

    # commit the data if a change was made
    if new_info:
        logger.debug('new venue info: %s: %s, %s, %s, %s', venue_row.id, venue_row.name, venue_row.city,
                     venue_row.capacity, venue_row.ageRestriction)
        session.commit()

    # close the session
    session.close()


def update_frmat(engine, frmat):
    """update a format to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	frmat (dict): dictionary for a format from the data source

    Returns:
    	None

    """
    logger.debug('Update format %s', frmat['id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the Format class from the formats table in the database
    Frmat = Base.classes.frmats

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # info changed flag
    new_info = False

    # query the format row
    frmat_row = session.query(Frmat).filter(Frmat.id == frmat['id']).first()

    # update the format details if necessary
    if frmat_row.name != frmat['name']:
        setattr(frmat_row, 'name', frmat['name'])
        new_info = True

    # commit the data if a change was made
    if new_info:
        logger.debug('new format info: %s: %s', frmat_row.id, frmat_row.name)
        session.commit()

    # close the session
    session.close()


def update_category(engine, category):
    """update a category to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	category (dict): dictionary for a category from the data source

    Returns:
    	None

    """
    logger.debug('Update category %s', category['id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the Category class from the categories table in the database
    Category = Base.classes.categories

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # info changed flag
    new_info = False

    # query the category row
    category_row = session.query(Category).filter(Category.id == category['id']).first()

    # update the category details if necessary
    if category_row.name != category['name']:
        setattr(category_row, 'name', category['name'])
        new_info = True

    # commit the data if a change was made
    if new_info:
        logger.debug('new category info: %s: %s', category_row.id, category_row.name)
        session.commit()

    # close the session
    session.close()


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
    with open(update_path, mode='r') as f:
        last_update_date = f.readline()

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
                if str(body) != "b''":
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

                    venue_ids = session.query(Venue.id).all()
                    venue_ids = {entity[0]: "" for entity in venue_ids}
                    logger.debug('%s venue ids retrieved', len(venue_ids))

                    # for each event in the events list of the output, check the event and venue
                    for event in output['events']:
                        # if the event id is in the current list, update it
                        if event['id'] in event_ids.keys():
                            update_event(engine, event, output['PullTime'])
                            num_events_updated += 1
                        # otherwise, create the event and add it to the list of objects to add
                        else:
                            objects_to_add.append(create_event(engine, event, output['PullTime']))
                            num_events_added += 1

                        # if the venue_id is in the current list, update it
                        if int(event['venue_id']) in venue_ids.keys():
                            update_venue(engine, event)
                            num_venues_updated += 1
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
