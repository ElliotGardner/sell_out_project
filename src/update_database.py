import json, requests  # import necessary libraries for intake of JSON results from eventbrite
from getpass import getpass  # import getpass for input of the token without showing it
from datetime import datetime  # import datetime for formatting of timestamps
from sqlalchemy import select, insert, Table, create_engine, MetaData, update  # import needed sqlalchemy libraries for db
import logging.config # import logging config

logging.config.fileConfig("config\\logging\\local.conf")
logger = logging.getLogger("update_database_log")

logger.debug('Start of update_database Script')

# set the oauth token
oauth = getpass(prompt='Enter the OAuth Personal Token for queries:')
headers = {
    'Authorization': ('Bearer ' + oauth),
}
logger.debug('OAuth token entered')

# pull new events
events = []
logger.debug('Retrieving Page 1...')
response_all = requests.get('https://www.eventbriteapi.com/v3/events/search/?categories=103&formats=5,6&location.address=chicago&location.within=50mi&sort_by=date&expand=venue,format,bookmark_info,ticket_availability,music_properties,guestlist_metrics,basic_inventory_info', headers = headers)
results_all = json.loads(response_all.text)
events = results_all['events']
logger.debug('has_more_items is %s', results_all['pagination']['has_more_items'])
page = 1
while results_all['pagination']['has_more_items']:
    page += 1
    logger.debug('Retrieving Page %s...', page)
    response_all = requests.get('https://www.eventbriteapi.com/v3/events/search/?categories=103&formats=5,6&location.address=chicago&location.within=50mi&sort_by=date&expand=venue,format,bookmark_info,ticket_availability,music_properties,guestlist_metrics,basic_inventory_info&page=' + str(page), headers = headers)
    results_all = json.loads(response_all.text)
    events += results_all['events']
    logger.debug('has_more_items is %s', results_all['pagination']['has_more_items'])
logger.info("%s pages received", page)
logger.info("%s events pulled", len(events))

# connect to the database and pull the current events table
logger.debug('Attempting connection with events.db')
engine = create_engine("sqlite:///data\\events.db")
connection = engine.connect()
metadata = MetaData()
events_table = Table('events', metadata, autoload=True, autoload_with=engine)

# query the ids of each event and create it as a list
query = select([events_table.c.id])
event_ids = [event[0] for event in list(connection.execute(query).fetchall())]

# print how many events are in the db
logger.info("%s events currently in the db", len(event_ids))

# build a list of the events in the pull whose event id is not in the database already
events_new = [event for event in events if int(event['id']) not in event_ids]

# print how many new events were in the pull
logger.info("%s new events from the pull", len(events_new))

# build a list of dictionaries with the necessary values for each new event from the eventbrite pull
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
                'ageRestriction':event['music_properties']['age_restriction'],
                'doorTime':event['music_properties']['door_time'],
                'presentedBy':event['music_properties']['presented_by'],
                'isOnline':event['online_event'],
                'allData':event}
               for event in events_new]

# if there are new events to add, then do so
if len(events_new) != 0:

    # insert into the events table
    stmt = insert(events_table)

    # execute the insert
    result_proxy = connection.execute(stmt, events_list)

    # print how many events were added to the db
    logger.info("%s new events added to the db", result_proxy.rowcount)

else:
    # if no insert is performed, the say so
    logger.info("0 new events added to the db")

# build a list of the events in the pull whose event id is already in the database
events_old = [event for event in events if int(event['id']) in event_ids]

# print how many events from the pull were already in the db
logger.info("%s old events from the pull", len(events_old))

# create a list of the ids of events already in the db
events_old_ids = [int(event['id']) for event in events_old]

# query the ids and sold out status of each event that is already in the db and create it as a dictionary
query = select([events_table.c.id, events_table.c.isSoldOut, events_table.c.soldOutDate])\
    .where(events_table.c.id.in_(events_old_ids))
event_sell_outs = {event[0]: {'soldOut': event[1], 'soldOutDate': event[2]} for event in
                   list(connection.execute(query).fetchall())}

# initialize a counter for the number of events whose sold out status changed
newSellOuts = 0

# for each event in the old events list, check if the sold out status has changed
for event in events_old:
    if event['ticket_availability']['is_sold_out'] != event_sell_outs[int(event['id'])]['soldOut']:
        # if the event sold out status has changed, update isSoldOut and change the date of sell out to today
        logger.debug("updating %s", event['id'])
        update_query = update(events_table).where(events_table.c.id == int(event['id'])).values(
            isSoldOut=event['ticket_availability']['is_sold_out'], soldOutDate=datetime.now())
        result = connection.execute(update_query)

        # print whether the query returned successfully or not
        if result.rowcount == 1:
            logger.info("updated %s", event['id'])
        else:
            logger.warning("problem updating %s", event['id'])

        # increment the newSellOuts counter
        newSellOuts += 1

# print how many events had a change in sold out status
logger.info("%s old events newly sold out", newSellOuts)

# close the connection to the database
connection.close()
logger.debug('DB connection closed')
