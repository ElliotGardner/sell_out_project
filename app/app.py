import traceback
from flask import render_template, request, redirect, url_for
import logging.config
from datetime import datetime

import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from sqlalchemy.orm import sessionmaker  # import the sessionmaker for adding data to the database
from sqlalchemy.ext.automap import automap_base # import for declaring classes

# Initialize the Flask application
app = Flask(__name__)

# Configure flask app from flask_config.py
app.config.from_pyfile('../config/flask_config.py')

# Define LOGGING_CONFIG in flask_config.py - path to config file for setting
# up the logger (e.g. config/logging/local.conf)
logging.config.fileConfig(app.config["LOGGING_CONFIG"])
logger = logging.getLogger("sell_out_project")
logger.debug('start of app')

# Initialize the database
db = SQLAlchemy(app)

engine = db.engine

# use the engine to build a reflection of the database
Base = automap_base()
Base.prepare(engine, reflect=True)

# build the classes from the tables in the database
Frmat = Base.classes.frmats
Category = Base.classes.categories
Event = Base.classes.events
Venue = Base.classes.venues
Feature = Base.classes.features
Score = Base.classes.scores

# create a session from the engine
session_mk = sessionmaker()
session_mk.configure(bind=engine)
session = session_mk()


@app.route('/')
def index():
    """Main view that lists events in the database.

    Create view into index page that uses data queried from the events, scores, and venues database and
    inserts it into the msiapp/templates/index.html template.

    Returns: rendered html template

    """

    try:
        results = session.query(Event, Venue, Score).join(Venue, Venue.id==Event.venueId).join(Score, Score.event_id==Event.id).filter(Event.startDate >= datetime.today()).limit(app.config["MAX_ROWS_SHOW"]).all()
        logger.debug("Index page accessed")
        return render_template('index.html', results=results)
    except:
        traceback.print_exc()
        logger.warning("Not able to display events, error page returned")
        return render_template('error.html')
