import sys # import sys for getting arguments from the command line call
from datetime import datetime  # import datetime for formatting of timestamps
from sqlalchemy import insert, Table, create_engine, MetaData, Column, String, Integer, Boolean, DATETIME, DECIMAL, JSON  # import needed sqlalchemy libraries for db
import logging.config # import logging config

logging.config.fileConfig("config\\logging\\local.conf")
logger = logging.getLogger("create_database_log")

logger.debug('Start of populate_database Script')


# connect to the database
connection = engine.connect()

# create a reflection of the events table in the database
events_table = Table('events', metadata, autoload = True, autoload_with = engine)

# build a list of dictionaries with the necessary values for each event from the eventbrite pull
events_list = [{'id':int(event['id']),
                'name':event['name']['text'],
                'startDate':datetime.fromisoformat(event['start']['local']),
                'endDate':datetime.fromisoformat(event['end']['local']),
                'publishedDate':datetime.fromisoformat(event['published'][0:19]),
                'onSaleDate': datetime.fromisoformat(event['ticket_availability']['start_sales_date']['local']) if event['ticket_availability']['start_sales_date'] is not None else datetime.fromisoformat(event['published'][0:19]),
                'venueId':int(event['venue_id']),
                'categoryId': int(event['subcategory_id']) if event['subcategory_id'] is not None else None,
                'formatId':int(event['format_id']),
                'inventoryType':event['inventory_type'],
                'isFree':event['is_free'],
                'isReservedSeating':event['is_reserved_seating'],
                'isAvailable':event['ticket_availability']['has_available_tickets'],
                'isSoldOut':event['ticket_availability']['is_sold_out'],
                'soldOutDate': datetime.now() if event['ticket_availability']['is_sold_out'] else datetime(2019,4,12,0,0,1),
                'hasWaitList':event['ticket_availability']['waitlist_available'],
                'minPrice': float(event['ticket_availability']['minimum_ticket_price']['major_value']) if event['ticket_availability']['minimum_ticket_price'] is not None else None,
                'maxPrice': float(event['ticket_availability']['maximum_ticket_price']['major_value']) if event['ticket_availability']['maximum_ticket_price'] is not None else None,
                'capacity': int(event['capacity']) if event['capacity'] is not None else 10000,
                'ageRestriction':event['music_properties']['age_restriction'],
                'doorTime':event['music_properties']['door_time'],
                'presentedBy':event['music_properties']['presented_by'],
                'isOnline':event['online_event'],
                'allData':event}
               for event in events]

#check that all events are populated, show where an issue might crop up
counter = 1
for event in events_list:
    logger.debug("Loaded Event %s", counter)
    counter += 1
    logger.debug("Event start date: %s ", event['startDate'])
    logger.debug("Event end date: %s ", event['endDate'])
    logger.debug("Event published date: %s ", event['publishedDate'])
    logger.debug("Event on sale date: %s ", event['onSaleDate'])
    logger.debug("Event min price: %s ", event['minPrice'])
    logger.debug("Event category: %s ", event['categoryId'])

# insert statement for the events table
stmt = insert(events_table)

# execute the insert into events
result_proxy = connection.execute(stmt, events_list)

# verify that the rows were inserted
logger.info("Inserted into Events Table %s events", result_proxy.rowcount)

# create a reflection of the venues table in the database
venues_table = Table('venues', metadata, autoload = True, autoload_with = engine)

# build a list of dictionaries with the necessary values for each venue from the eventbrite pull
venues_list = []  # generate a blank venues list
venues_ids_list = []  # generate a blank list of venues ids
logger.debug("Blank venues list created")
for event in events:  # loop through each event
    logger.debug("Event %s checked", event['id'])
    if event['venue_id'] not in venues_ids_list:  # if the venue id for the event hasn't been encountered yet
        venues_ids_list.append(event['venue_id'])  # add the id to the venues ids list

        # create a dictionary for the venue info passed by the event pull
        venue = {'id':int(event['venue_id']),
                'name':event['venue']['name'],
                'city':event['venue']['address']['city'],
                'ageRestriction': event['venue']['age_restriction'],
                'capacity': int(event['venue']['capacity']) if event['venue']['capacity'] is not None else 10000
                }

        venues_list.append(venue)  # append the venue dictionary to the venues list

# insert statement for the venues table
stmt = insert(venues_table)

# execute the insert into venues
result_proxy = connection.execute(stmt, venues_list)

# verify that the rows were inserted
logger.info("Inserted into Venues Table %s venues", result_proxy.rowcount)

# create a reflection of the categories table in the database
categories_table = Table('categories', metadata, autoload = True, autoload_with = engine)

# pull subcategories
subcategories = []
logger.debug('Retrieving subcategories...')
response_cats = requests.get('https://www.eventbriteapi.com/v3/subcategories/', headers = headers)
results_cats = json.loads(response_cats.text)
subcategories = results_cats['subcategories']
logger.debug('has_more_items is %s', results_cats['pagination']['has_more_items'])
page = 1
while results_cats['pagination']['has_more_items']:
    page += 1
    logger.debug('Retrieving Page %s...', page)
    response_cats = requests.get('https://www.eventbriteapi.com/v3/subcategories/?continuation=' + results_cats['pagination']['continuation'], headers = headers)
    results_cats = json.loads(response_cats.text)
    subcategories += results_cats['subcategories']
    logger.debug('has_more_items is %s', results_all['pagination']['has_more_items'])
logger.info("%s pages received", page)
logger.info("%s subcategories pulled", len(subcategories))

# build a list of dictionaries with the necessary values for each subcategory from the eventbrite pull
subs_list = [{'id':int(sub['id']),
              'name':sub['name']}
             for sub in subcategories]

# insert statement for the categories table
stmt = insert(categories_table)

# execute the insert into categories
result_proxy = connection.execute(stmt, subs_list)

# verify that the rows were inserted
logger.info("Inserted into Categories Table %s categories", result_proxy.rowcount)

# create a reflection of the formats table in the database
formats_table = Table('formats', metadata, autoload = True, autoload_with = engine)

# pull formats
formats = []
logger.debug('Retrieving formats...')
response_formats = requests.get('https://www.eventbriteapi.com/v3/formats/', headers = headers)
results_formats = json.loads(response_formats.text)
formats = results_formats['formats']
logger.info("%s formats pulled", len(formats))

# build a list of dictionaries with the necessary values for each subcategory from the eventbrite pull
formats_list = [{'id':int(form['id']),
                'name':form['name']}
               for form in formats]

# insert statement for the formats table
stmt = insert(formats_table)

# execute the insert into formats
result_proxy = connection.execute(stmt, formats_list)

# verify that the rows were inserted
logger.info("Inserted into Formats Table %s formats", result_proxy.rowcount)

# close the connection to the database
connection.close()
logger.debug('DB connection closed')
