import os
import sys  # import sys for getting arguments from the command line call
from getpass import getpass  # import getpass for input of the token without showing it
import logging.config  # import logging config
from datetime import datetime  # import datetime for building folder paths
import json, requests  # import necessary libraries for intake of JSON results from eventbrite

from sqlalchemy import create_engine # import needed sqlalchemy library for db engine creation
from sqlalchemy import Column, String, Integer, Boolean, DATETIME, DECIMAL  # import needed sqlalchemy libraries for db
from sqlalchemy.ext.declarative import declarative_base  # import for declaring classes
from sqlalchemy.ext.automap import automap_base # import for declaring classes
from sqlalchemy.orm import sessionmaker  # import the sessionmaker for adding data to the database
import pandas as pd
 
configPath = os.path.join("config","logging","local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("helpers")


def set_headers(oauth_token=None):
    """get the OAuth token needed for an API connection and set the header for the connection

    Args:
    	oauth_token (str): Optionally provide the OAuth token

    Returns:
    	headers (dict): The headers needed for the API request

    """
    logger.info("Setting API Request Headers")
    # if the oauth_token wasn't passed, then request the user enter one
    if oauth_token is None:
        oauth_token = getpass(prompt='Enter the OAuth Personal Token for API requests:')
        logger.debug("OAuth token set to %s", oauth_token)
    else:
        logger.debug("OAuth token provided in function call as %s", oauth_token)

    # set the token into a requests header
    headers = {
        'Authorization': ('Bearer ' + oauth_token),
    }
    logger.debug('Headers created')

    # return the headers
    return headers


def API_request(API_url, page_num=1, headers=None):
    """makes a single API request to a page for a given page number and headers

    Args:
    	API_url (str): the URL for the API request
    	page_num(int): the current page being requested
    	headers (dict): the headers to use for the API call

    Returns:
    	results (dict): the data from the API call response

    """
    logger.info('Retrieving Page %s...', page_num)

    # if the page number is one, use the plain API call, otherwise add the specified page to the API url
    if page_num == 1:
        full_url = API_url
    else:
        full_url = API_url + "&page=" + str(page_num)
    logger.debug("URL for the call is %s", full_url)

    # make the API call and load the response text as a JSON dictionary
    response = requests.get(
        full_url,
        headers=headers)
    logger.debug("Received response of %s", response.status_code)
    # if a bad response (not 200) is received, then stop the load
    if response.status_code != requests.codes.ok:
        logger.error("Bad Response!")
        sys.exit()
    results = json.loads(response.text)
    try:
        logger.debug("Received results containing %s events", len(results['events']))
    except Exception as e:
        logger.debug("Received a response, but no events contained: %s", e)

    # append the time of pull to the results
    date = datetime.now()
    results['PullTime'] = date.strftime('%y-%m-%d-%H-%M-%S')

    # return the results JSON
    return results


def create_db_engine(database_name, type):
    """Create an engine for a specific database and database type

    Args:
    	database_name (str): the name of the database to create
    	type (str): the type of database to create

    Returns:
        engine (SQLAlchemy engine): the engine for working with a database

    """

    # generate the engine_string based on the name and database type
    if type == "sqlite":
        # set up sqlite connection
        engine_string = type + ":///" + database_name

    elif type == "mysql+pymysql":
        # set up mysql connection
        # the engine_string format
        # engine_string = "{type}:///{user}:{password}@{host}:{port}/{database}"
        user = os.environ.get("MYSQL_USER")
        password = os.environ.get("MYSQL_PASSWORD")
        host = os.environ.get("MYSQL_HOST")
        port = os.environ.get("MYSQL_PORT")
        engine_string = "{}://{}:{}@{}:{}/{}".format(type, user, password, host, port, database_name)

    # if the type of database wasn't set to mysql_pymysql or sqlite, then log an error and exit
    else:
        logger.error("Type of database provided wasn't supported: %s", type)
        sys.exit()

    logger.debug("Engine string is %s", engine_string)
    # create the engine
    engine = create_engine(engine_string)

    # return the engine
    return engine


def create_event(engine, event, infoDate):
    """make an event to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	event (dict): dictionary for an event from the data source
    	infoDate (str): the date the info is from (pull date)

    Returns:
    	event_to_add (Event): the built event to push to the database

    """
    logger.debug("Creating event %s to add to the database", event['id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the Event class from the events table in the database
    Event = Base.classes.events

    # build the event
    event_to_add = Event(id=event['id'],
                name=event['name']['text'],
                startDate=datetime.fromisoformat(event['start']['local']),
                endDate=datetime.fromisoformat(event['end']['local']),
                publishedDate=datetime.fromisoformat(event['published'][0:19]),
                onSaleDate= datetime.fromisoformat(event['ticket_availability']['start_sales_date']['local']) if event['ticket_availability']['start_sales_date'] is not None else datetime.fromisoformat(event['published'][0:19]),
                venueId=int(event['venue_id']),
                categoryId= int(event['subcategory_id']) if event['subcategory_id'] is not None else 3999,  # this is the "music other" subcategory
                formatId=int(event['format_id']) if event['format_id'] is not None else 100,  # this is the "other" format
                inventoryType=event['inventory_type'],
                isFree=event['is_free'],
                isReservedSeating=event['is_reserved_seating'],
                isAvailable=event['ticket_availability']['has_available_tickets'],
                isSoldOut=event['ticket_availability']['is_sold_out'],
                soldOutDate= datetime.strptime(infoDate, '%y-%m-%d-%H-%M-%S') if event['ticket_availability']['is_sold_out'] else datetime(2019,4,12,0,0,1),
                hasWaitList=event['ticket_availability']['waitlist_available'],
                minPrice= float(event['ticket_availability']['minimum_ticket_price']['major_value']) if event['ticket_availability']['minimum_ticket_price'] is not None else None,
                maxPrice= float(event['ticket_availability']['maximum_ticket_price']['major_value']) if event['ticket_availability']['maximum_ticket_price'] is not None else None,
                capacity= int(event['capacity']) if event['capacity'] is not None else 10000,
                ageRestriction=event['music_properties']['age_restriction'],
                doorTime=event['music_properties']['door_time'],
                presentedBy=event['music_properties']['presented_by'],
                isOnline=event['online_event'],
                url=event['url'],
                lastInfoDate=datetime.strptime(infoDate, '%y-%m-%d-%H-%M-%S'))
    logger.debug("Event %s was created", event_to_add.id)

    # return the event
    return event_to_add


def create_venue(engine, event):
    """make a venue to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	event (dict): dictionary for a event (with venue info) from the data source

    Returns:
    	venue_to_add (Venue): the built venue to push to the database

    """
    logger.debug("Creating venue %s to add to the database", event['venue_id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
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


def create_frmat(engine, frmat):
    """make a format to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	frmat (dict): dictionary for a format from the data source

    Returns:
    	frmat_to_add (Format): the built format to push to the database

    """
    logger.debug("Creating format %s to add to the database", frmat['id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the Format class from the formats table in the database
    Frmat = Base.classes.frmats

    # build the format
    frmat_to_add = Frmat(id=int(frmat['id']),
                name=frmat['name'])
    logger.debug("Format %s was created", frmat_to_add.id)

    # return the format
    return frmat_to_add


def create_category(engine, category):
    """make a category to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	category (dict): dictionary for a category from the data source

    Returns:
    	category_to_add (Category): the built category to push to the database

    """
    logger.debug("Creating category %s to add to the database", category['id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the Category class from the categories table in the database
    Category = Base.classes.categories

    # build the category
    category_to_add = Category(id=int(category['id']),
                name=category['name'])
    logger.debug("Category %s was created", category_to_add.id)

    # return the category
    return category_to_add


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


def event_to_event_dict(event):
    """helper function for converting an event into a dictionary for comparisons"""
    event_dict = {}
    event_dict['id'] = event['id']
    event_dict['name'] = event['name']['text']
    event_dict['startDate'] = datetime.fromisoformat(event['start']['local'])
    event_dict['endDate'] = datetime.fromisoformat(event['end']['local'])
    event_dict['publishedDate'] = datetime.fromisoformat(event['published'][0:19])
    event_dict['onSaleDate'] = datetime.fromisoformat(
        event['ticket_availability']['start_sales_date']['local']) if event['ticket_availability'][
                                                                                                           'start_sales_date'] is not None else datetime.fromisoformat(
        event['published'][0:19])
    event_dict['venueId'] = int(event['venue_id'])
    event_dict['categoryId'] = int(event['subcategory_id']) if event[
                                                                   'subcategory_id'] is not None else 3999  # this is the "music other" subcategory
    event_dict['formatId'] = int(event['format_id']) if event[
                                                            'format_id'] is not None else 100  # this is the "other" format
    event_dict['inventoryType'] = event['inventory_type']
    event_dict['isFree'] = int(event['is_free'])
    event_dict['isReservedSeating'] = int(event['is_reserved_seating'])
    event_dict['isAvailable'] = int(event['ticket_availability']['has_available_tickets'])
    event_dict['isSoldOut'] = int(event['ticket_availability']['is_sold_out'])
    event_dict['hasWaitList'] = int(event['ticket_availability']['waitlist_available'])
    event_dict['minPrice'] = float(event['ticket_availability']['minimum_ticket_price']['major_value']) if \
        event['ticket_availability']['minimum_ticket_price'] is not None else None
    event_dict['maxPrice'] = float(event['ticket_availability']['maximum_ticket_price']['major_value']) if \
        event['ticket_availability']['maximum_ticket_price'] is not None else None
    event_dict['capacity'] = int(event['capacity']) if event['capacity'] is not None else 10000
    event_dict['ageRestriction'] = event['music_properties']['age_restriction']
    event_dict['doorTime'] = event['music_properties']['door_time']
    event_dict['presentedBy'] = event['music_properties']['presented_by']
    event_dict['isOnline'] = int(event['online_event'])

    return event_dict


def event_to_venue_dict(event):
    """helper function for converting a venue into a dictionary for comparisons"""
    venue_dict = {}
    venue_dict['id'] = int(event['venue_id'])
    if event['venue'] is not None:
        venue_dict['name'] = event['venue']['name']
        venue_dict['city'] = event['venue']['address']['city']
        if event['venue']['capacity'] is not None:
            venue_dict['capacity'] = int(event['venue']['capacity'])
        else:
            venue_dict['capacity'] = 10000
        venue_dict['ageRestriction'] = event['venue']['age_restriction']
    else:
        venue_dict['name'] = None
        venue_dict['city'] = None
        venue_dict['capacity'] = None
        venue_dict['ageRestriction'] = None

    return venue_dict


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


def create_feature(engine, feature):
    """make an feature to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	feature (dict): dictionary for an feature from the data source

    Returns:
    	feature_to_add (Feature): the built feature to push to the database

    """
    logger.debug("Creating feature %s to add to the database", feature['id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the feature class from the features table in the database
    Feature = Base.classes.features

    # build the feature
    feature_to_add = Feature(id=feature['id'],
        startDate=datetime.fromisoformat(feature['startDate']) if type(feature['startDate']) == str else feature['startDate'].to_pydatetime(),
        categoryId=feature['categoryId'],
        formatId=feature['formatId'],
        inventoryType=feature['inventoryType'],
        isFree=feature['isFree'],
        isReservedSeating=feature['isReservedSeating'],
        minPrice=feature['minPrice'],
        maxPrice=feature['maxPrice'],
        venueName_simple=feature['venueName_simple'],
        onSaleWindow=feature['onSaleWindow'],
        eventWeekday=feature['eventWeekday'],
        startHour=feature['startHour'],
        capacity=feature['capacity'],
        locale=feature['locale'],
        ageRestriction=feature['ageRestriction'],
        presentedBy_simple=feature['presentedBy_simple'],
        isSoldOut=feature['isSoldOut'],
        soldOutLead=feature['soldOutLead'])

    logger.debug("feature %s was created", feature_to_add.id)

    # return the feature
    return feature_to_add


def update_feature(engine, feature):
    """update a feature to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	feature (dict): dictionary for an feature from the data source

    Returns:
    	None

    """
    logger.debug('Update feature %s', feature['id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the feature class from the features table in the database
    Feature = Base.classes.features

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # info changed flag
    new_info = False

    # query the feature row
    feature_row = session.query(Feature).filter(Feature.id == feature['id']).first()

    # update the feature details if necessary
    if feature_row.startDate != datetime.fromisoformat(feature['startDate']) if type(feature['startDate']) == str else feature['startDate'].to_pydatetime():
        setattr(feature_row, 'startDate', datetime.fromisoformat(feature['startDate']) if type(feature['startDate']) == str else feature['startDate'].to_pydatetime())
        new_info = True
        logging.debug('new startDate')

    if feature_row.categoryId != feature['categoryId']:
        setattr(feature_row, 'categoryId', feature['categoryId'])
        new_info = True
        logging.debug('new category')

    if feature_row.formatId != feature['formatId']:
        setattr(feature_row, 'formatId', feature['formatId'])
        new_info = True
        logging.debug('new format id')

    if feature_row.inventoryType != feature['inventoryType']:
        setattr(feature_row, 'inventoryType', feature['inventoryType'])
        new_info = True
        logging.debug('new inventory type')

    if feature_row.isFree != feature['isFree']:
        setattr(feature_row, 'isFree', feature['isFree'])
        new_info = True
        logging.debug('new isFree')

    if feature_row.isReservedSeating != feature['isReservedSeating']:
        setattr(feature_row, 'isReservedSeating', feature['isReservedSeating'])
        new_info = True
        logging.debug('new isReservedSeating')

    if feature_row.minPrice != feature['minPrice']:
        setattr(feature_row, 'minPrice', feature['minPrice'])
        new_info = True
        logging.debug('new min price')

    if feature_row.maxPrice != feature['maxPrice']:
        setattr(feature_row, 'maxPrice', feature['maxPrice'])
        new_info = True
        logging.debug('new max price')

    if feature_row.venueName_simple != feature['venueName_simple']:
        setattr(feature_row, 'venueName_simple', feature['venueName_simple'])
        new_info = True
        logging.debug('new venue name')

    if feature_row.onSaleWindow != feature['onSaleWindow']:
        setattr(feature_row, 'onSaleWindow', feature['onSaleWindow'])
        new_info = True
        logging.debug('new on sale window')

    if feature_row.eventWeekday != feature['eventWeekday']:
        setattr(feature_row, 'eventWeekday', feature['eventWeekday'])
        new_info = True
        logging.debug('new event weekday')

    if feature_row.startHour != feature['startHour']:
        setattr(feature_row, 'startHour', feature['startHour'])
        new_info = True
        logging.debug('new on start hour')

    if feature_row.capacity != feature['capacity']:
        setattr(feature_row, 'capacity', feature['capacity'])
        new_info = True
        logging.debug('new capacity')

    if feature_row.locale != feature['locale']:
        setattr(feature_row, 'locale', feature['locale'])
        new_info = True
        logging.debug('new locale')

    if feature_row.ageRestriction != feature['ageRestriction']:
        setattr(feature_row, 'ageRestriction', feature['ageRestriction'])
        new_info = True
        logging.debug('new age restriction')

    if feature_row.presentedBy_simple != feature['presentedBy_simple']:
        setattr(feature_row, 'presentedBy_simple', feature['presentedBy_simple'])
        new_info = True
        logging.debug('new presented by')

    if feature_row.isSoldOut != feature['isSoldOut']:
        setattr(feature_row, 'isSoldOut', feature['isSoldOut'])
        new_info = True
        logging.debug('new isSoldOut')

    if feature_row.soldOutLead != feature['soldOutLead']:
        setattr(feature_row, 'soldOutLead', feature['soldOutLead'])
        new_info = True
        logging.debug('new sold out lead')

    # commit the data if a change was made
    if new_info:
        logger.debug('new feature: %s: %s, %s', feature_row.id, feature_row.isSoldOut, feature_row.soldOutLead)
        session.commit()

    # close the session
    session.close()


def pull_features(engine):
    """function for pulling features from a populated and updated database for training a model

    Given a database connection engine, access the database and pull the requested
    data as a Pandas dataframe.

    Args:
        engine (SQLAlchemy engine): the engine for working with a database

    Returns:
        features (pandas DataFrame): a dataframe containing the features columns for each event

    """
    logger.debug('Start of pull features function')

    features = pd.read_sql('SELECT * FROM features', engine)
    features['startDate'] = features['startDate'].apply(lambda x: datetime.fromisoformat(x) if type(x) == str else x.to_pydatetime())

    logger.debug('%s', features.head())

    return features


def create_scores_table(engine):
    """function for creating a scores table in a database

    Given a database connection engine, access the database and create a scores table

    Args:
        engine (SQLAlchemy engine): the engine for working with a database

    Returns:
        None

    """
    # check if the scores table already exists, stop execution if it does
    if 'scores' in engine.table_names():
        logging.warning('scores table already exists!')

    else:
        logger.debug("Creating a scores table at %s", engine.url)

        Base = declarative_base()

        logger.debug("Creating the scores table")

        # create a score class
        class Score(Base):
            """Create a data model for the scores table """
            __tablename__ = 'scores'
            pred_id = Column(String(24), primary_key=True)
            event_id = Column(String(12), unique=False, nullable=False)
            startDate = Column(DATETIME(), unique=False, nullable=False)
            predictionDate = Column(DATETIME(), unique=False, nullable=False)
            willSellOut = Column(Boolean(), unique=False, nullable=False)
            confidence = Column(DECIMAL(), unique=False, nullable=False)
            howFarOut = Column(DECIMAL(), unique=False, nullable=False)

            def __repr__(self):
                return '<Score %r>' % self.id

        try:
            # create the tables
            Base.metadata.create_all(engine)

            # check that the tables were created
            for table in engine.table_names():
                logger.info("Created table %s", table)
        except Exception as e:
            logger.error("Could not create the database: %s", e)


def create_score(engine, score):
    """make a score to add to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	score (dict): dictionary for a score from the data source

    Returns:
    	score_to_add (Score): the built score to push to the database

    """
    logger.debug("Creating score %s to add to the database", score['pred_id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the score class from the scores table in the database
    Score = Base.classes.scores

    # build the score
    score_to_add = Score(pred_id=score['pred_id'],
        event_id=score['id'],
        startDate=datetime.fromisoformat(score['startDate']) if type(score['startDate']) == str else score['startDate'].to_pydatetime(),
        predictionDate=datetime.today(),
        willSellOut=score['willSellOut'],
        confidence=float(score['confidence']),
        howFarOut=score['howFarOut'])

    logger.debug("score %s was created", score_to_add.pred_id)

    # return the score
    return score_to_add


def update_score(engine, score):
    """update a score to the database using an engine

    Args:
    	engine (SQLAlchemy engine): the engine for working with a database
    	score (dict): dictionary for an score from the data source

    Returns:
    	None

    """
    logger.debug('Update score %s', score['pred_id'])

    # use the engine to build a reflection of the database
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    # build the score class from the scores table in the database
    Score = Base.classes.scores

    # create a session from the engine
    session_mk = sessionmaker()
    session_mk.configure(bind=engine)
    session = session_mk()

    # info changed flag
    new_info = False

    # query the score row
    score_row = session.query(Score).filter(Score.pred_id == score['pred_id']).first()

    # update the score details if necessary
    if score_row.willSellOut != score['willSellOut']:
        setattr(score_row, 'willSellOut', score['willSellOut'])
        new_info = True
        logging.debug('new willSellOut')

    if score_row.confidence != float(score['confidence']):
        setattr(score_row, 'confidence', float(score['confidence']))
        new_info = True
        logging.debug('new confidence')

    if score_row.howFarOut != score['howFarOut']:
        setattr(score_row, 'howFarOut', score['howFarOut'])
        new_info = True
        logging.debug('new howFarOut')

    # commit the data if a change was made
    if new_info:
        logger.debug('new score: %s: %s, %s, %s, %s', score_row.pred_id, score_row.startDate, score_row.willSellOut, score_row.confidence, score_row.howFarOut)
        session.commit()

    # close the session
    session.close()