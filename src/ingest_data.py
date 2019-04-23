import json, requests  # import necessary libraries for intake of JSON results from eventbrite
import sys  # import sys for getting arguments from the command line call
import os  # import os for writing JSON to a file
from datetime import datetime  # import datetime for building folder paths
from getpass import getpass  # import getpass for input of the token without showing it
import logging.config  # import logging config

logging.config.fileConfig("config\\logging\\local.conf")
logger = logging.getLogger("ingest_data_log")

logger.debug('Start of ingest_data Script')

# check if a token argument was passed
if len(sys.argv) > 1:
    oauth = sys.argv[1]
else: # if no additional arguments were passed, then prompt for a token
    oauth = getpass(prompt='Enter the OAuth Personal Token for queries:')

# set the token into a requests header
headers = {
    'Authorization': ('Bearer ' + oauth),
}
logger.debug('OAuth token entered')

# save the current date and time
date = datetime.now()
logger.debug('Current date and time is %s', date.isoformat())

# pull new events
logger.debug('Retrieving Page 1...')
response_all = requests.get('https://www.eventbriteapi.com/v3/events/search/?categories=103&formats=5,6&location.address=chicago&location.within=50mi&sort_by=date&expand=venue,format,bookmark_info,ticket_availability,music_properties,guestlist_metrics,basic_inventory_info', headers = headers)
results = json.loads(response_all.text)
results['PullTime'] = date.strftime('%y-%m-%d-%H-%M-%S')

page = 1

# create a directory to save the raw data to
folder = os.getcwd() + "\\data\\raw\\" + str(date.year) + "\\" + str(date.month) + "\\" + str(date.day) + "\\"
logger.debug("Attempting to create folder %s", folder)
try:
    os.makedirs(os.path.dirname(folder))
    logger.info("Folder %s created", folder)
except OSError:
    logger.info("Folder %s already exists", folder)

# save the JSON file
filename = folder + str(date.hour) + "_" + str(date.minute) + "_" + str(date.second) + "_" + str(page) + ".json"
logger.debug("Writing to file %s", filename)
with open(filename, "w") as json_file:
    json.dump(results, json_file)
    logger.info("File %s written", filename)

logger.debug('has_more_items is %s', results['pagination']['has_more_items'])

# if the response says more pages exist, pull the next page
while results['pagination']['has_more_items']:
    page += 1
    logger.debug('Retrieving Page %s...', page)
    response_all = requests.get('https://www.eventbriteapi.com/v3/events/search/?categories=103&formats=5,6&location.address=chicago&location.within=50mi&sort_by=date&expand=venue,format,bookmark_info,ticket_availability,music_properties,guestlist_metrics,basic_inventory_info&page=' + str(page), headers = headers)
    results = json.loads(response_all.text)
    results['PullTime'] = date.strftime('%y-%m-%d-%H-%M-%S')

    # save the JSON file
    filename = folder + str(date.hour) + "_" + str(date.minute) + "_" + str(date.second) + "_" + str(page) + ".json"
    logger.debug("Writing to file %s", filename)
    with open(filename, "w") as json_file:
        json.dump(results, json_file)
        logger.info("File %s written", filename)

    logger.debug('has_more_items is %s', results['pagination']['has_more_items'])

logger.info("%s pages received", page)
