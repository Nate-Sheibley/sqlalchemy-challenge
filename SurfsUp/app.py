# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, text

from flask import Flask, jsonify
#################################################
# Database Setup
#################################################

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

def get_most_recent_date():
    date_format = "%Y-%m-%d"
    most_recent_measurement_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    most_recent_date = dt.datetime.strptime(most_recent_measurement_date[0], date_format)
    return most_recent_date

def convert_date(date):
    date_format = "%Y-%m-%d"
    date = dt.datetime.strptime(date, date_format)
    return date

def get_most_active_station():
    most_active = session.query(*[Measurement.station, func.count(Measurement.station).label("count")]).\
                    group_by(Measurement.station).\
                    order_by(text("count DESC")).\
                    first()
    return most_active[0]

#################################################
# Flask Routes
#################################################

@app.route("/")
def root():
    result = """Available paths: \n
        /api/v1.0/precipitation \n
        /api/v1.0/stations \n
        /api/v1.0/tobs \n
        /api/v1.0/<start> \n
        /api/v1.0/<start>/<end>
        """
    return result

@app.route("/api/v1.0/precipitation")
def precip():
    # query to retrieve the last 12 months of precipitation data and plot the results. 
    # Starting from the most recent data point in the database. 
    most_recent_date = get_most_recent_date()

    # Calculate the date one year from the last date in data set.
    one_year_ago = most_recent_date - dt.timedelta(days=365)

    measurement_columns = [Measurement.date, Measurement.prcp]
    # Perform a query to retrieve the data and precipitation scores
    prev_year_query = session.query(*measurement_columns).\
            filter(Measurement.date > one_year_ago).all()
    prev_year_dict = dict(prev_year_query)
    return jsonify(prev_year_dict)
    

@app.route("/api/v1.0/stations")
def station_list():
    stations = session.query(Station.station).all()
    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def temp_observed():
    most_recent_date = get_most_recent_date()
    one_year_ago = most_recent_date - dt.timedelta(days=365)
    most_active = get_most_active_station()

    twelve_month_temp = session.query(Measurement.tobs).\
        filter(Measurement.date > one_year_ago).\
        filter(Measurement.station == most_active).\
        all()

    return jsonify(twelve_month_temp)

@app.route("/api/v1.0/<start>")
def min_max_avg_T(start):
    most_active = get_most_active_station()
    start = convert_date(start)

    min_max_avg_temp = session.query(*[func.min(Measurement.tobs).label("min"),
                                   func.max(Measurement.tobs).label("max"),
                                   func.avg(Measurement.tobs).label("avg")]).\
                            filter(Measurement.station == most_active).\
                            filter(Measurement.date >= start).\
                            first()
    return jsonify(min_max_avg_temp)


@app.route("/api/v1.0/<start>/<end>")
def min_max_avg_T_2ended(start, end):
    most_active = get_most_active_station()
    start = convert_date(start)
    end = convert_date(end)

    min_max_avg_temp = session.query(*[func.min(Measurement.tobs).label("min"),
                                    func.max(Measurement.tobs).label("max"),
                                    func.avg(Measurement.tobs).label("avg")]).\
                            filter(Measurement.station == most_active).\
                            filter(Measurement.date >= start).\
                            filter(Measurement.date <= end).\
                            first()
    return jsonify(min_max_avg_temp)