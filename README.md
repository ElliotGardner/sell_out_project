Instructions:

To access data from the eventbrite API, you need an API key:
1. To obtain one, login (or create an account) at https://www.eventbrite.com/signin/?referrer=%2Faccount-settings%2Fapps
2. After logging in, navigate from the top bar to "account settings"
3. On the left, there is a dropdown for "Developer Links"
4. Select "API Keys" from this menu
5. Click "Create API Key" and fill out the requested information (I used this location of my repo for the site url)
6. Once granted, use the "Private API Key" or "personal oauth token" when an "API_token" is requested within this code repo

In order to get started, a few environmental variables must be set up.
1. Export the top-level directory for the repo (wherever it is cloned to) to the environmental "PYTHONPATH" variable using the command `export PYTHONPATH=<location of repo top level>`.
2. Reset the "last_update.txt" file in the "config" folder to a date prior to the ingest of your first data, maintaining the same date format. I used "90-01-01-01-01-01".
3. If you are using a mysql database or saving data to s3, then you need to also export your mysql and establish your aws configurations.
	- For MySQL, at a minimum call `export MYSQL_HOST = <host>`, `export MYSQL_USER= <user>`, `export MYSQL_PORT = <port>`, and `export MYSQL_PASSWORD = <password>` with your relevant info
	- For AWS, follow this guide: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html

config/config.yml holds the key configurations for running all of the scripts without needing to pass arguments beyond the API key. If you wish to adjust settings, ensure that you change the settings in multiple locations if the same field is repeated (for instance, raw_data_location)

Once all that has been completed, the `make all` function should construct the app in it's entirety, to include setting up a virtual environment, ingesting the data from the API, creating the database, populating the data, building models, and evaluation of results.

For a daily update, the `make daily` command will run only the 'update', 'features', 'train', 'score', and 'evaluate' portions of the app.

Data can be downloaded from the API directly, however, no historical data is provided. Instead, I have hosted the historical data that I have gathered over the past two months into a public AWS S3 bucket, at 'emg8426.msia423.project' in the 'raw' folder. The app is currently configured to use a subset of data from the 'data/sample' folder (which will be quicker), but alternatively the config.yml file can be changed to reflect the S3 bucket in order to ingest all the data that I had access to.

In order to serve up the app as a Flask-supported website, adjust the settings in config/flask_config.py as necessary, and then call `python run.py app`.

Lastly, testing is accomplished either through "make test" or calling `pytest` from the command line from the top-level of the repo.


# Project Charter

* **Vision**: Help concertgoers make informed decisions regarding when to buy concert tickets through predicting which shows will sell out. This will give them time to find out if friends and family are interested in attending as well, without fear of encountering a sold-out event by the time they've finished coordinating a group outing. This will increase marginal utility for users, and allow more social-influencing activity, increasing the chances of exposing a larger set of people to artists, venues, and events.
* **Mission**: Build an app which shows a user upcoming events in their area, and gives real-time predictions on whether a particular show will sell out. Data will be gathered from the Eventbrite API, and predictions will be provided through a classification model (potentially a Random Forest or Boosted Tree).
* **Success Criteria**: 
  * Machine-Learning: We want to consider both the precision and recall of our model, as the recall (ability to successfully identify sell-outs) provides value to a user directly, while the precision (ability to accurately predict only true sell-outs, not over-predict) can provide value to potential partners (increase the chances of exposure of new people to artists, venues, and events by interested users). As such, we want to evaluate our machine learning model through the F1 score, which combines both precision and recall metrics. If we were able to get a 70% for both precision and recall, that would yield an *F1 score of .7*, so we would seek to meet or exceed this threshold.
  * Business Outcome: If we were successful in providing value to users on giving predictions for upcoming events, we would expect to see users more consistently buy tickets and more often buy more than one ticket (given that they had time to coordinate with others instead of worrying about being able to get a ticket before it sold out). In order to be successful, we would want this increase in mean tickets sold within a given period for users of the app to be *5% or greater*.
  
# Planning

### Event Listings and Prediction

* Data Gathering: Building the pipeline for connecting to the data source (API) and ingesting the data
  * API Integration: I can access event data from the Eventbrite API
  * Data Capture: I can store data from Eventbrite for easy access later
  * Data Formatting: I have labeled data that is appropriate for building a classification model
  
* Sell-Out Prediction: Building a model for predicting whether an event will sell-out prior to the start date
  * Logistic Model: I have a tuned logistic model for classifying events as selling out or not
  * Neural Network: I have a tuned neural network for classifying events
  * Boosted Tree: I have a tuned boosted tree for classifying events
  * Random Forest: I have a tuned random forest for classifying events
  * Model Selection: I have a model which performs best according to ML metrics and for latency considerations
 
### User Interaction

* Web App Construction: Establishing the web app and necessary back-end components
  * Database Hosting: The web app can access the data necessary for re-training a model and updating predictions
  * Model Hosting: The web app can access the selected model, re-train as necessary, and retrieve predictions
  * Interface Building: The web app has a front-end that can be interacted with

* Event Selection: Constructing an interface for users to see and select events of interest
  * Event Search: The web app can pull a list of events for given criteria from the eventbrite API
  * Event Selection: A user can select an event from the list and see the details, along with the prediction of selling-out or not

### New Features

* Spotify Artist Popularity Incorporation

* Days Until Sell Out Prediction

* User Event Watchlist

* Customized User Event Digest
 
# Backlog
 
1. Events Listings and Prediction - Data Gathering - API Integration (1 pt) - COMPLETED
2. Events Listings and Prediction - Data Gathering - Data Gathering (2 pts) - COMPLETED
3. Events Listings and Prediction - Data Gathering - Data Formatting (2 pts) - COMPLETED
4. Events Listings and Prediction - Sell-Out Prediction - Logistic Model (1 pt) - COMPLETED
5. Events Listings and Prediction - Sell-Out Prediction - Boosted Tree (1 pt) - COMPLETED
6. Events Listings and Prediction - Sell-Out Prediction - Random Forest (1 pt) - CANCELED
7. Events Listings and Prediction - Sell-Out Prediction - Neural Network (1 pt) - CANCELED
8. Events Listings and Prediction - Sell-Out Prediction - Model Selection (2 pts) - COMPLETED
9. User Interaction - Web App Construction - Database Hosting (4 pts) - COMPLETED
10. User Interaction - Web App Construction - Model Hosting (4 pts) - COMPLETED
11. User Interaction - Web App Construction - Interface Building - COMPLETED
12. New Features - Days Until Sell Out Prediction - COMPLETED
 
# Icebox

* User Interaction - Event Selection - Event Search
* User Interaction - Event Selection - Event Selection
* New Features - Spotify Artist Popularity Incorporation
* New Features - User Event Watchlist
* New Features - Customized User Event Digest