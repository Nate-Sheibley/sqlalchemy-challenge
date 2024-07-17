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
print(Base.classes.keys())
Measurement = Base.classes.measurement
Station = Base.classes.station


#################################################
# Flask Setup
#################################################

app = Flask(__name__)
print(app)

#################################################
# Helper functions to reduce rewriting code
#################################################

def get_most_recent_date():
    #open query session from python to db
    session = Session(engine)
    #query and related info
    date_format = "%Y-%m-%d"
    most_recent_measurement_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    #close query session
    session.close()
    
    most_recent_date = dt.datetime.strptime(most_recent_measurement_date[0], date_format)
    return most_recent_date

def convert_date(date):
    date_format = "%Y-%m-%d"
    new_date = dt.datetime.strptime(date, date_format)
    return new_date

def get_most_active_station():
    #open query session from python to db
    session = Session(engine)

    #query and related info
    most_active = session.query(*[Measurement.station,
                                  func.count(Measurement.station).label("count")]).\
                    group_by(Measurement.station).\
                    order_by(text("count DESC")).\
                    first()

    #close query session
    session.close()

    return most_active[0]

def fix_type_to_jsonable(result):
    fixed = [tuple(row) for row in result]
    return fixed

#################################################
# Flask Routes
#################################################

@app.route("/")
def root():
    """Available paths:"""
    return (
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )

@app.route("/api/v1.0/precipitation")
def precip():
    print('running precipitation page')
    # query to retrieve the last 12 months of precipitation data and plot the results.
    # Starting from the most recent data point in the database.
    most_recent_date = get_most_recent_date()

    # Calculate the date one year from the last date in data set.
    one_year_ago = most_recent_date - dt.timedelta(days=365)

    #open query session from python to db
    session = Session(engine)

    #query and related info
    measurement_columns = [Measurement.date, Measurement.prcp]
    # Perform a query to retrieve the data and precipitation scores
    prev_year_query = session.query(*measurement_columns).\
            filter(Measurement.date > one_year_ago).all()

    #close query session
    session.close()

    prev_year_dict = dict(prev_year_query)
    return jsonify(prev_year_dict)


@app.route("/api/v1.0/stations")
def station_list():
    print('running stations page')
    #open query session from python to db
    session = Session(engine)

    #query and related info
    stations = session.query(Station.station).all()

    #close query session
    session.close()

    return jsonify(fix_type_to_jsonable(stations))

@app.route("/api/v1.0/tobs")
def temp_observed():
    print('running temperatures page')
    most_recent_date = get_most_recent_date()
    one_year_ago = most_recent_date - dt.timedelta(days=365)
    
    most_active = get_most_active_station()
    print(f'Results are from most active station: {most_active}')

    #open query session from python to db
    session = Session(engine)

    #query and related info
    twelve_month_temp = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date > one_year_ago).\
        filter(Measurement.station == most_active).\
        all()

    #close query session
    session.close()

    return jsonify(fix_type_to_jsonable(twelve_month_temp))

@app.route("/api/v1.0/<start>")
def min_max_avg_T(start):
    # start is expected to be format "%Y-%m-%d"
    print('running extremes page, to present')
    most_active = get_most_active_station()
    start = convert_date(start)

    # open query session from python to db
    session = Session(engine)

    # query and related info
    min_max_avg_temp = session.query(*[func.min(Measurement.tobs).label("min"),
                                   func.max(Measurement.tobs).label("max"),
                                   func.avg(Measurement.tobs).label("avg")]).\
                            filter(Measurement.station == most_active).\
                            filter(Measurement.date >= start).\
                            first()
    
    #close query session
    session.close()

    return jsonify(min_max_avg_temp)


@app.route("/api/v1.0/<start>/<end>")
def min_max_avg_T_2ended(start, end):
    # start and end are expected to be format "%Y-%m-%d"
    print('running extremes page, start - end')
    most_active = get_most_active_station()
    start = convert_date(start)
    end = convert_date(end)

    #open query session from python to db
    session = Session(engine)

    #query and related info
    min_max_avg_temp = session.query(*[func.min(Measurement.tobs).label("min"),
                                    func.max(Measurement.tobs).label("max"),
                                    func.avg(Measurement.tobs).label("avg")]).\
                            filter(Measurement.station == most_active).\
                            filter(Measurement.date >= start).\
                            filter(Measurement.date <= end).\
                            first()

    #close query session
    session.close()

    return jsonify(min_max_avg_temp)

if __name__ == "__main__":
    app.run()