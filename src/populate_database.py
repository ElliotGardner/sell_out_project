import os
import sys  # import sys for getting arguments from the command line call
import argparse  # import argparse for getting arguments from the command line
import yaml  # import yaml for pulling config file
from datetime import datetime  # import datetime for formatting of timestamps
from sqlalchemy.orm import sessionmaker  # import the sessionmaker for adding data to the database
from sqlalchemy.ext.declarative import declarative_base  # import for declaring classes
import logging.config  # import logging config

from src.create_database import create_engine  # import a function for creating an engine
from src.ingest_data import API_request, set_headers  # import for initially populating events, venues, formats, and categories

configPath = os.path.join("config","logging","local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("populate_database_log")


def create_event(engine, event):
    """make an event to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	event (dict): dictionary for an event from the API pull

    Returns:
    	event_to_add (Event): the built event to push to the database

    """
    logger.debug("Creating event %s to add to the database", event['id'])

    # use the engine to build a reflection of the database
    Base = declarative_base()
    Base.prepare(engine, reflect=True)

    # build the Event class from the events table in the database
    Event = Base.classes.events

    # build the event
    event_to_add = Event(id=int(event['id']),
                name=event['name']['text'],
                startDate=datetime.fromisoformat(event['start']['local']),
                endDate=datetime.fromisoformat(event['end']['local']),
                publishedDate=datetime.fromisoformat(event['published'][0:19]),
                onSaleDate= datetime.fromisoformat(event['ticket_availability']['start_sales_date']['local']) if event['ticket_availability']['start_sales_date'] is not None else datetime.fromisoformat(event['published'][0:19]),
                venueId=int(event['venue_id']),
                categoryId= int(event['subcategory_id']) if event['subcategory_id'] is not None else None,
                formatId=int(event['format_id']),
                inventoryType=event['inventory_type'],
                isFree=event['is_free'],
                isReservedSeating=event['is_reserved_seating'],
                isAvailable=event['ticket_availability']['has_available_tickets'],
                isSoldOut=event['ticket_availability']['is_sold_out'],
                soldOutDate= datetime.now() if event['ticket_availability']['is_sold_out'] else datetime(2019,4,12,0,0,1),
                hasWaitList=event['ticket_availability']['waitlist_available'],
                minPrice= float(event['ticket_availability']['minimum_ticket_price']['major_value']) if event['ticket_availability']['minimum_ticket_price'] is not None else None,
                maxPrice= float(event['ticket_availability']['maximum_ticket_price']['major_value']) if event['ticket_availability']['maximum_ticket_price'] is not None else None,
                capacity= int(event['capacity']) if event['capacity'] is not None else 10000,
                ageRestriction=event['music_properties']['age_restriction'],
                doorTime=event['music_properties']['door_time'],
                presentedBy=event['music_properties']['presented_by'],
                isOnline=event['online_event'])
    logger.debug("Event %s was created", event_to_add.id)

    # return the event
    return event_to_add


def create_venue(engine, event):
    """make a venue to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	event (dict): dictionary for a event (with venue info) from the API pull

    Returns:
    	venue_to_add (Venue): the built venue to push to the database

    """
    logger.debug("Creating venue %s to add to the database", event['venue_id'])

    # use the engine to build a reflection of the database
    Base = declarative_base()
    Base.prepare(engine, reflect=True)

    # build the Venue class from the venues table in the database
    Venue = Base.classes.venues

    # build the venue
    venue_to_add = Venue(id=int(event['venue_id']),
                name=event['venue']['name'],
                city=event['venue']['address']['city'],
                ageRestriction=event['venue']['age_restriction'],
                capacity=int(event['venue']['capacity']) if event['venue']['capacity'] is not None else 10000)
    logger.debug("Venue %s was created", venue_to_add.id)

    # return the venue
    return venue_to_add


def create_format(engine, format):
    """make a format to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	format (dict): dictionary for a format from the API pull

    Returns:
    	format_to_add (Format): the built format to push to the database

    """
    logger.debug("Creating format %s to add to the database", format['id'])

    # use the engine to build a reflection of the database
    Base = declarative_base()
    Base.prepare(engine, reflect=True)

    # build the Format class from the formats table in the database
    Format = Base.classes.formats

    # build the format
    format_to_add = Format(id=int(format['id']),
                name=format['name'])
    logger.debug("Format %s was created", format_to_add.id)

    # return the format
    return format_to_add


def create_category(engine, category):
    """make a category to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	category (dict): dictionary for a category from the API pull

    Returns:
    	category_to_add (Category): the built category to push to the database

    """
    logger.debug("Creating category %s to add to the database", category['id'])

    # use the engine to build a reflection of the database
    Base = declarative_base()
    Base.prepare(engine, reflect=True)

    # build the Category class from the categories table in the database
    Category = Base.classes.categories

    # build the category
    category_to_add = Category(id=int(category['id']),
                name=category['name'])
    logger.debug("Category %s was created", category_to_add.id)

    # return the category
    return category_to_add


def update_event(engine, event):
    """update an event to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	event (dict): dictionary for an event from the API pull

    Returns:
    	None

    """
    logger.debug('Update event %s', event['id'])

    # use the engine to build a reflection of the database
    Base = declarative_base()
    Base.prepare(engine, reflect=True)

    # build the Event class from the events table in the database
    Event = Base.classes.events

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # query the event row
    event_row = session.query(Event).filter(Event.id == event['id']).first()
    logger.debug('old event: %', event_row)

    # update the event details in full
    setattr(event_row, 'name', event['name']['text'])
    setattr(event_row, 'startDate', datetime.fromisoformat(event['start']['local']))
    setattr(event_row, 'endDate', datetime.fromisoformat(event['end']['local']))
    setattr(event_row, 'publishedDate', datetime.fromisoformat(event['published'][0:19]))
    setattr(event_row, 'onSaleDate', datetime.fromisoformat(event['ticket_availability']['start_sales_date']['local']) if \
                 event['ticket_availability']['start_sales_date'] is not None else datetime.fromisoformat(
        event['published'][0:19]))
    setattr(event_row, 'venueId', int(event['venue_id']))
    setattr(event_row, 'categoryId', int(event['subcategory_id']) if event['subcategory_id'] is not None else None)
    setattr(event_row, 'formatId', int(event['format_id']))
    setattr(event_row, 'inventoryType', event['inventory_type'])
    setattr(event_row, 'isFree', event['is_free'])
    setattr(event_row, 'isReservedSeating', event['is_reserved_seating'])
    setattr(event_row, 'isAvailable', event['ticket_availability']['has_available_tickets'])
    setattr(event_row, 'isSoldOut', event['ticket_availability']['is_sold_out'])
    setattr(event_row, 'soldOutDate', datetime.now() if event['ticket_availability']['is_sold_out'] else datetime(2019, 4, 12, 0, 0, 1))
    setattr(event_row, 'hasWaitList', event['ticket_availability']['waitlist_available'])
    setattr(event_row, 'minPrice', float(event['ticket_availability']['minimum_ticket_price']['major_value']) if \
               event['ticket_availability']['minimum_ticket_price'] is not None else None)
    setattr(event_row, 'maxPrice', float(event['ticket_availability']['maximum_ticket_price']['major_value']) if \
               event['ticket_availability']['maximum_ticket_price'] is not None else None)
    setattr(event_row, 'capacity', int(event['capacity']) if event['capacity'] is not None else 10000)
    setattr(event_row, 'ageRestriction', event['music_properties']['age_restriction'])
    setattr(event_row, 'doorTime', event['music_properties']['door_time'])
    setattr(event_row, 'presentedBy', event['music_properties']['presented_by'])
    setattr(event_row, 'isOnline', event['online_event'])
    logger.debug('new event: %s', event_row)


    session.commit()


def update_venue(engine, event):
    """update a venue to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	event (dict): dictionary for an event (which contains a venue) from the API pull

    Returns:
    	None

    """
    logger.debug('Update venue %s', event['venue_id'])

    # use the engine to build a reflection of the database
    Base = declarative_base()
    Base.prepare(engine, reflect=True)

    # build the Venue class from the venues table in the database
    Venue = Base.classes.venues

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # query the event row
    venue_row = session.query(Venue).filter(Venue.id == event['venue_id']).first()
    logger.debug('old venue: %s', venue_row)

    # update the venue details in full
    setattr(venue_row, 'name', event['venue']['name'])
    setattr(venue_row, 'city', event['venue']['address']['city'])
    setattr(venue_row, 'capacity', int(event['venue']['capacity']) if event['venue']['capacity'] is not None else 10000)
    setattr(venue_row, 'ageRestriction', event['venue']['age_restriction'])
    logger.debug('new venue: %s', venue_row)

    session.commit()


def initial_populate(engine, events_URL, formats_URL, categories_URL, headers=None):
    """a function for generating an initial set of data for the database

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	events_URL (str): the URL for the API call to get events (covers venues too)
    	formats_URL (str): the URL for the API call to get formats
    	categories_URL (str): the URL for the API call to get categories
    	headers (dict): the headers to use for the API call

    Returns:
    	num_events (int): number of events in the database
    	num_venues (int): number of venues in the database
    	num_formats (int): number of formats in the database
    	num_categories (int): number of categories in the database

    """
    logger.debug('Start of populate database function')

    # use the engine to build a reflection of the database
    Base = declarative_base()
    Base.prepare(engine, reflect=True)

    # build the classes from the tables in the database
    Event = Base.classes.events
    Venue = Base.classes.venues
    Format = Base.classes.formats
    Category = Base.classes.categories

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # create a flag for whether the events table was pulled
    events_pulled = False

    # initialize a list of objects to add to the database
    objects_to_add = []

    # query the events table
    num_events = session.query(Event).count()
    logger.debug('Number of existing events: %', num_events)

    # if the events table has no results, then make an API call to get them
    if num_events == 0:
        # get the events results from the API
        events = API_request(events_URL, headers=headers)

        # change the events_pulled flag to true
        events_pulled = True

        # for each event returned, create an Event for the database and add it to the list of objects to add
        for event in events['events']:
            objects_to_add.append(create_event(engine, event))


    # query the venues table
    num_venues = session.query(Venue).count()
    logger.debug('Number of existing venues: %', num_venues)

    # if the venues table has no results, then make an API call to get them
    if num_venues == 0:
        if not events_pulled:
            # initialize a list of events
            events = []

            # get the events results from the API
            results = API_request(events_URL, headers=headers)

        # for each event returned, create a Venue for the database and add it to the list of objects to add
        for event in events['events']:
            objects_to_add.append(create_venue(engine, event))

    # query the formats table
    num_formats = session.query(Format).count()
    logger.debug('Number of existing formats: %', num_formats)

    # if the formats table has no results, then make an API call to get them
    if num_formats == 0:
        # get the formats results from the API
        formats = API_request(formats_URL, headers=headers)

        # for each format returned, create an Event for the database and add it to the list of objects to add
        for format in format['formats']:
            objects_to_add.append(create_event(engine, event))

    # query the categories table
    num_categories = session.query(Category).count()
    logger.debug('Number of existing categories: %', num_categories)

    # if the categories table has no results, then make an API call to get them
    if num_categories == 0:

        # get the categories results from the API
        page = 1
        results = API_request(categories_URL, headers=headers)

        # subcategories = []
        # logger.debug('Retrieving subcategories...')
        # response_cats = requests.get('https://www.eventbriteapi.com/v3/subcategories/', headers = headers)
        # results_cats = json.loads(response_cats.text)
        # subcategories = results_cats['subcategories']
        # logger.debug('has_more_items is %s', results_cats['pagination']['has_more_items'])
        # page = 1
        # while results_cats['pagination']['has_more_items']:
        #     page += 1
        #     logger.debug('Retrieving Page %s...', page)
        #     response_cats = requests.get('https://www.eventbriteapi.com/v3/subcategories/?continuation=' + results_cats['pagination']['continuation'], headers = headers)
        #     results_cats = json.loads(response_cats.text)
        #     subcategories += results_cats['subcategories']
        #     logger.debug('has_more_items is %s', results_all['pagination']['has_more_items'])
        # logger.info("%s pages received", page)
        # logger.info("%s subcategories pulled", len(subcategories)



# connect to the database
connection = engine.connect()

logger.debug("Creating session using engine for %s", engine.url)
    # creating the sessionmaker and session
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

session.add(event_to_add)
    session.commit()
    logger.debug("Event %s added", event['id'])
    session.close()

# # connect to the database
# connection = engine.connect()
#
# # create a reflection of the events table in the database
# events_table = Table('events', metadata, autoload = True, autoload_with = engine)
#
# # build a list of dictionaries with the necessary values for each event from the eventbrite pull
# events_list = [{'id':int(event['id']),
#                 'name':event['name']['text'],
#                 'startDate':datetime.fromisoformat(event['start']['local']),
#                 'endDate':datetime.fromisoformat(event['end']['local']),
#                 'publishedDate':datetime.fromisoformat(event['published'][0:19]),
#                 'onSaleDate': datetime.fromisoformat(event['ticket_availability']['start_sales_date']['local']) if event['ticket_availability']['start_sales_date'] is not None else datetime.fromisoformat(event['published'][0:19]),
#                 'venueId':int(event['venue_id']),
#                 'categoryId': int(event['subcategory_id']) if event['subcategory_id'] is not None else None,
#                 'formatId':int(event['format_id']),
#                 'inventoryType':event['inventory_type'],
#                 'isFree':event['is_free'],
#                 'isReservedSeating':event['is_reserved_seating'],
#                 'isAvailable':event['ticket_availability']['has_available_tickets'],
#                 'isSoldOut':event['ticket_availability']['is_sold_out'],
#                 'soldOutDate': datetime.now() if event['ticket_availability']['is_sold_out'] else datetime(2019,4,12,0,0,1),
#                 'hasWaitList':event['ticket_availability']['waitlist_available'],
#                 'minPrice': float(event['ticket_availability']['minimum_ticket_price']['major_value']) if event['ticket_availability']['minimum_ticket_price'] is not None else None,
#                 'maxPrice': float(event['ticket_availability']['maximum_ticket_price']['major_value']) if event['ticket_availability']['maximum_ticket_price'] is not None else None,
#                 'capacity': int(event['capacity']) if event['capacity'] is not None else 10000,
#                 'ageRestriction':event['music_properties']['age_restriction'],
#                 'doorTime':event['music_properties']['door_time'],
#                 'presentedBy':event['music_properties']['presented_by'],
#                 'isOnline':event['online_event'],
#                 'allData':event}
#                for event in events]
#
# #check that all events are populated, show where an issue might crop up
# counter = 1
# for event in events_list:
#     logger.debug("Loaded Event %s", counter)
#     counter += 1
#     logger.debug("Event start date: %s ", event['startDate'])
#     logger.debug("Event end date: %s ", event['endDate'])
#     logger.debug("Event published date: %s ", event['publishedDate'])
#     logger.debug("Event on sale date: %s ", event['onSaleDate'])
#     logger.debug("Event min price: %s ", event['minPrice'])
#     logger.debug("Event category: %s ", event['categoryId'])
#
# # insert statement for the events table
# stmt = insert(events_table)
#
# # execute the insert into events
# result_proxy = connection.execute(stmt, events_list)
#
# # verify that the rows were inserted
# logger.info("Inserted into Events Table %s events", result_proxy.rowcount)
#
# # create a reflection of the venues table in the database
# venues_table = Table('venues', metadata, autoload = True, autoload_with = engine)
#
# # build a list of dictionaries with the necessary values for each venue from the eventbrite pull
# venues_list = []  # generate a blank venues list
# venues_ids_list = []  # generate a blank list of venues ids
# logger.debug("Blank venues list created")
# for event in events:  # loop through each event
#     logger.debug("Event %s checked", event['id'])
#     if event['venue_id'] not in venues_ids_list:  # if the venue id for the event hasn't been encountered yet
#         venues_ids_list.append(event['venue_id'])  # add the id to the venues ids list
#
#         # create a dictionary for the venue info passed by the event pull
#         venue = {'id':int(event['venue_id']),
#                 'name':event['venue']['name'],
#                 'city':event['venue']['address']['city'],
#                 'ageRestriction': event['venue']['age_restriction'],
#                 'capacity': int(event['venue']['capacity']) if event['venue']['capacity'] is not None else 10000
#                 }
#
#         venues_list.append(venue)  # append the venue dictionary to the venues list
#
# # insert statement for the venues table
# stmt = insert(venues_table)
#
# # execute the insert into venues
# result_proxy = connection.execute(stmt, venues_list)
#
# # verify that the rows were inserted
# logger.info("Inserted into Venues Table %s venues", result_proxy.rowcount)
#
# # create a reflection of the categories table in the database
# categories_table = Table('categories', metadata, autoload = True, autoload_with = engine)
#
# # pull subcategories
# subcategories = []
# logger.debug('Retrieving subcategories...')
# response_cats = requests.get('https://www.eventbriteapi.com/v3/subcategories/', headers = headers)
# results_cats = json.loads(response_cats.text)
# subcategories = results_cats['subcategories']
# logger.debug('has_more_items is %s', results_cats['pagination']['has_more_items'])
# page = 1
# while results_cats['pagination']['has_more_items']:
#     page += 1
#     logger.debug('Retrieving Page %s...', page)
#     response_cats = requests.get('https://www.eventbriteapi.com/v3/subcategories/?continuation=' + results_cats['pagination']['continuation'], headers = headers)
#     results_cats = json.loads(response_cats.text)
#     subcategories += results_cats['subcategories']
#     logger.debug('has_more_items is %s', results_all['pagination']['has_more_items'])
# logger.info("%s pages received", page)
# logger.info("%s subcategories pulled", len(subcategories))
#
# # build a list of dictionaries with the necessary values for each subcategory from the eventbrite pull
# subs_list = [{'id':int(sub['id']),
#               'name':sub['name']}
#              for sub in subcategories]
#
# # insert statement for the categories table
# stmt = insert(categories_table)
#
# # execute the insert into categories
# result_proxy = connection.execute(stmt, subs_list)
#
# # verify that the rows were inserted
# logger.info("Inserted into Categories Table %s categories", result_proxy.rowcount)
#
# # create a reflection of the formats table in the database
# formats_table = Table('formats', metadata, autoload = True, autoload_with = engine)
#
# # pull formats
# formats = []
# logger.debug('Retrieving formats...')
# response_formats = requests.get('https://www.eventbriteapi.com/v3/formats/', headers = headers)
# results_formats = json.loads(response_formats.text)
# formats = results_formats['formats']
# logger.info("%s formats pulled", len(formats))
#
# # build a list of dictionaries with the necessary values for each subcategory from the eventbrite pull
# formats_list = [{'id':int(form['id']),
#                 'name':form['name']}
#                for form in formats]
#
# # insert statement for the formats table
# stmt = insert(formats_table)
#
# # execute the insert into formats
# result_proxy = connection.execute(stmt, formats_list)
#
# # verify that the rows were inserted
# logger.info("Inserted into Formats Table %s formats", result_proxy.rowcount)
#
# # close the connection to the database
# connection.close()
# logger.debug('DB connection closed')

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
    logger.debug('Start of populate_database Script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the type and location of the db
    parser = argparse.ArgumentParser(description="create database")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--type', default=None, help="type of database to create")
    parser.add_argument('--database_name', default=None, help="location where database is to be created (including name.db)")

    args = parser.parse_args()

    # run the creation based on the parsed arguments
    run_create(args)
