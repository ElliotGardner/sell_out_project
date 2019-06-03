import json, requests  # import necessary libraries for intake of JSON results from eventbrite
import sys  # import sys for getting arguments from the command line call
import argparse  # import argparse to get arguments at the command call
import os  # import os for writing JSON to a file
import yaml  # import yaml for loading config file
from datetime import datetime  # import datetime for building folder paths
from getpass import getpass  # import getpass for input of the token without showing it
import logging.config  # import logging config
import boto3  # interact with s3
import botocore.exceptions # an error category for S3 functionality

configPath = os.path.join("config","logging","local.conf")
logging.config.fileConfig(configPath)
logger = logging.getLogger("ingest_data_log")


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


def save_JSON_s3(JSON_data, filename, bucket, folder = "", public=False):
    """saves a JSON file to an s3 bucket under a given name

    Args:
    	JSON_data (dict): a dictionary representing a JSON file to upload
    	filename (str): the name to save the JSON file as in s3
    	bucket (str): the name of the bucket to push to
    	folder (str): the structure of the folders above the file to append to the filename, should end in "/"
        public (bool): if the file should be made public or not

    Returns:
    	None

    """
    logging.info("Uploading %s to %s", filename, bucket)

    # create an s3 resource
    s3 = boto3.resource('s3')

    try:  # try creating the object
        # concatenate the folder structure and filename
        fullname = folder + filename
        logger.debug("Concatenated filename to %s", fullname)

        # create the s3 object
        obj = s3.Object(bucket, fullname)

        # if the public flag is True, then set the permissions to public
        if public:
            # put the JSON_data into the body of the object
            response = obj.put(Body=json.dumps(JSON_data),ACL='public-read')
            logger.info("JSON uploaded as %s", response["ETag"])
            logger.info("Object set to public-read permission")
        # if the public flag is False, then don't change the permission
        else:
            # put the JSON_data into the body of the object
            response = obj.put(Body=json.dumps(JSON_data))
            logger.info("JSON uploaded as %s", response["ETag"])

    except Exception as e:
        logger.error(e)


def save_JSON_local(JSON_data, filename, folder=None, local_location=None):
    """saves a JSON file to a local filesystem under a given name

    Args:
    	JSON_data (dict): a dictionary representing a JSON file to upload
    	filename (str): the name to save the JSON file as in the local filestructure
    	folder (str): the structure of the folders to save the file into
    	local_location (str): the name of the overall location to push to

    Returns:
    	None

    """
    # if a local_location isn't provided, use the current working directory
    if local_location is None:
        local_location = os.getcwd()

    logging.info("Uploading %s to %s", filename, local_location)

    # if a folder is provided, then append it to the local location and create the folder
    if folder is not None:
        # create the directory to save the raw data to
        overall_dir = os.path.join(local_location, folder)
        logger.debug("Attempting to create folder %s", overall_dir)
        try:
            os.makedirs(os.path.dirname(overall_dir))
            logger.info("Folder %s created", overall_dir)
        except OSError as e:
            logger.debug("Folder %s already exists", overall_dir)
    # if no folder is provided, then use the current directory as the directory
    else:
        overall_dir = local_location

    # create the overall filepath to save to
    overall_filename = os.path.join(overall_dir, filename)

    # save the file
    logger.debug("Writing to file %s", overall_filename)
    try:
        with open(overall_filename, "w") as file:
            json.dump(JSON_data, file)
            logger.info("File %s written", filename)
    except Exception as e:
        logger.info("Problem writing %s: %s", overall_filename, e)


def run_ingest(args):
    """runs the ingest script"""
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

    # get the API url from the config file
    if "ingest_data" in config and "API_url" in config["ingest_data"]:
        API_url = config["ingest_data"]["API_url"]
        logger.debug("API url is %s", API_url)

    else:  # if no URL is provided, then log the error and exit
        logger.error("'API_url' must be passed in 'ingest_data' within the config file")
        sys.exit()

    # initialize the flags for the output types of the ingest
    s3_save = False
    local_save = False

    # set initial API parameters
    page = 1
    more_pages = True
    date = datetime.now()

    # do different things based on how the save is specified
    if "ingest_data" in config and "how" in config["ingest_data"]:
        # if the how method is 'both' then trigger the s3 and local save actions
        if config["ingest_data"]["how"] == "both":
            s3_save = True
            local_save = True
        # if the how method is 's3' then trigger the s3 actions
        elif config["ingest_data"]["how"] == "s3":
            s3_save = True
            local_save = False
        # if the how method is 'local' then trigger the local save actions
        elif config["ingest_data"]["how"] == "local":
            s3_save = False
            local_save = True
        # if the how method is 'test' then don't save, but continue to call the requests
        elif config["ingest_data"]["how"] == "test":
            s3_save = False
            local_save = False
        else:  # if neither "both" nor "s3" nor "local" nor "test" was specified, log an error and exit
            logger.error("'how' in the 'ingest_data' within the config file needs to specify 'both', 's3', or 'local'")
            sys.exit()

    # if saving to s3, then get the relevant info
    if s3_save:
        # get the S3 bucket from the config file
        if "ingest_data" in config and "s3_bucket" in config["ingest_data"]:
            s3_bucket = config["ingest_data"]["s3_bucket"]
            logger.debug("s3 bucket is %s", s3_bucket)

        else:  # if no s3 bucket is provided, then log the error and exit
            logger.error("'s3_bucket' must be passed in 'ingest_data' within the config file")
            sys.exit()

        # create a folder (prefix of the filename in S3) to save the raw data to
        s3_folder = "raw/" + str(date.year) + "/" + str(date.month) + "/" + str(date.day) + "/"
        logger.debug('s3 folder set to %s', s3_folder)

        # get the S3 public status from the config file
        if "ingest_data" in config and "s3_public" in config["ingest_data"]:
            s3_public = config["ingest_data"]["s3_public"]

        else:  # if no s3 public status is provided, then default to False
            s3_public = False

        logger.debug("s3 public status is %s", s3_public)

    # if saving to local, then get the relevant info
    if local_save:
        # get the local data folder from the config file
        if "ingest_data" in config and "output_folder" in config["ingest_data"]:
            folder = config["ingest_data"]["output_folder"]

        else:  # if no output folder is provided, then use "data" as the default
            folder = "data"

        # create the local folder to save the raw data to
        local_folder = os.path.join(folder, "raw", str(date.year), str(date.month), str(date.day), "")
        logger.debug('local folder set to %s', local_folder)

    # while loop to pull all pages and save them
    while more_pages:
        # get the results from the API call
        results = API_request(API_url, page, headers)

        # create the filename to use
        filename = str(date.hour) + "_" + str(date.minute) + "_" + str(date.second) + "_" + str(page) + ".json"
        logger.debug('filename set to %s', filename)

        if s3_save:
            # save to S3
            save_JSON_s3(results, filename, s3_bucket, s3_folder, public=s3_public)

        if local_save:
            # save to local
            save_JSON_local(results, filename, local_folder)

        # increment the page number and more_pages flag
        page += 1
        more_pages = results['pagination']['has_more_items']
        logger.debug('has_more_items is %s', results['pagination']['has_more_items'])

    logger.info("%s pages received", page-1)

if __name__ == '__main__':
    logger.debug('Start of ingest_data script')

    # if this code is run as a script, then parse arguments for the location of the config and, optionally, the API OAuth token
    parser = argparse.ArgumentParser(description="ingest data")
    parser.add_argument('--config', help='path to yaml file with configurations')
    parser.add_argument('--API_token', default=None, help="API OAuth Token for API calls")

    args = parser.parse_args()

    # run the ingest based on the parsed arguments
    run_ingest(args)

    print("Done!")
