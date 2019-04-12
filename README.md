# Project Charter

* **Vision**: Help concertgoers make informed decisions regarding when to buy concert tickets, giving them time to find out if friends and family are interested in attending as well, without fear of encountering a sold-out event by the time they've finished coordinating a group outing. This will increase marginal utility for users, and allow more social-influencing activity, increasing the chances of exposing a larger set of people to artists, venues, and events.
* **Mission**: Build an app which shows a user upcoming events in their area, and gives real-time predictions on whether a particular show will sell out.
* **Success Criteria**: 
  * Machine-Learning: We want to consider both the precision and recall of our model, as the recall (ability to successfully identify sell-outs) provides value to a user directly, while the precision (ability to accurately predict only true sell-outs, not over-predict) can provide value to potential partners (increase the chances of exposure of new people to artists, venues, and events by interested users). As such, we want to evaluate our machine learning model through the F1 score, which combines both precision and recall metrics. If we were able to get a 70% for both precision and recall, that would yield an F1 score of .7, so we would seek to meet or exceed this threshold.
  * Business Outcome: If we were successful in providing value to users on giving predictions for upcoming events, we would expect to see users more consistently buy tickets and more often buy more than one ticket (given that they had time to coordinate with others instead of worrying about being able to get a ticket before it sold-out). In order to be successful, we would want this increase in mean tickets sold within a given period for users of the app to be 5% or higher.
  
# Planning

### Event Listings and Prediction

* Data Gathering: Building the pipeline for connecting to the data source (API) and ingesting the data
  * API Integration: I can access event data from the Eventbrite API
  * Data Capture: I can store data from Eventbrite for easy access later
  * Data Formatting: I have labeled data that is appropriate for building a classification model
  
* Sell-Out Prediction: Building a model for predicting whether an event will sell-out prior to the start date
  * Logistic Model: I have a basic logistic model for classifying events as selling out or not
  * Neural Network: I have a neural network for classifying events
  * Boosted Tree: I have a boosted tree for classifying events
  * Random Forest: I have a random forest for classifying events
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

* Days Until Sell Out Prediction

* User Event Watchlist

* Customized User Event Digest
 
# Backlog
 
1. Events Listings and Prediction - Data Gathering - API Integration (1 pt) - PLANNED
2. Events Listings and Prediction - Data Gathering - Data Gathering (2 pts) - PLANNED
3. Events Listings and Prediction - Data Gathering - Data Formatting (2 pts) - PLANNED
4. Events Listings and Prediction - Sell-Out Prediction - Logistic Model (1 pt) - PLANNED
5. Events Listings and Prediction - Sell-Out Prediction - Boosted Tree (1 pt) - PLANNED
6. Events Listings and Prediction - Sell-Out Prediction - Random Forest (1 pt) - PLANNED
7. Events Listings and Prediction - Sell-Out Prediction - Neural Network (1 pt) - PLANNED
8. Events Listings and Prediction - Sell-Out Prediction - Model Selection (2 pts) - PLANNED
9. User Interaction - Web App Construction - Database Hosting (4 pts)
10. User Interaction - Web App Construction - Model Hosting (4 pts)
 
# Icebox

* User Interaction - Web App Construction - Interface Hosting
* User Interaction - Event Selection - Event Search
* User Interaction - Event Selection - Event Selection
* Days Until Sell Out Prediction
* User Event Watchlist
* Customized User Event Digest
