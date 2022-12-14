

def create_virtual_warehouse(session):
    session.sql("CREATE WAREHOUSE IF NOT EXISTS citibike_wh WITH WAREHOUSE_SIZE='X-SMALL'").collect()
    
def create_database(session):
    session.sql("CREATE DATABASE IF NOT EXISTS citibike_db").collect()

def create_schema(session):
    session.use_warehouse('citibike_wh')
    session.use_database('citibike_db')
    session.sql("CREATE OR REPLACE SCHEMA citibike_sc").collect()

def create_tables(session):
    session.sql("CREATE OR REPLACE TABLE citibike_db.citibike_sc.citibike \
             ( \
                ride_id VARCHAR(16), \
                rideable_type VARCHAR(16), \
                started_at TIMESTAMP_NTZ, \
                ended_at TIMESTAMP_NTZ, \
                start_station_name VARCHAR(64), \
                start_station_id VARCHAR(16), \
                end_station_name VARCHAR(64), \
                end_station_id VARCHAR(16), \
                start_lat VARCHAR(32), \
                start_lng VARCHAR(32), \
                end_lat VARCHAR(32), \
                end_lng VARCHAR(32), \
                member_casual VARCHAR(16) \
            )").collect()

    session.sql("CREATE OR REPLACE TABLE citibike_db.citibike_sc.citibike_dq_passed \
                ( \
                    ride_id VARCHAR(16), \
                    rideable_type VARCHAR(16), \
                    started_at TIMESTAMP_NTZ, \
                    ended_at TIMESTAMP_NTZ, \
                    start_station_name VARCHAR(64), \
                    start_station_id VARCHAR(16), \
                    end_station_name VARCHAR(64), \
                    end_station_id VARCHAR(16), \
                    start_lat VARCHAR(32), \
                    start_lng VARCHAR(32), \
                    end_lat VARCHAR(32), \
                    end_lng VARCHAR(32), \
                    member_casual VARCHAR(16) \
                )").collect()

    session.sql("CREATE OR REPLACE TABLE citibike_db.citibike_sc.citibike_dq_bad_nulls \
                ( \
                    ride_id VARCHAR(16), \
                    rideable_type VARCHAR(16), \
                    started_at TIMESTAMP_NTZ, \
                    ended_at TIMESTAMP_NTZ, \
                    start_station_name VARCHAR(64), \
                    start_station_id VARCHAR(16), \
                    end_station_name VARCHAR(64), \
                    end_station_id VARCHAR(16), \
                    start_lat VARCHAR(32), \
                    start_lng VARCHAR(32), \
                    end_lat VARCHAR(32), \
                    end_lng VARCHAR(32), \
                    member_casual VARCHAR(16) \
                )").collect()

    session.sql("CREATE OR REPLACE TABLE citibike_db.citibike_sc.citibike_dq_bad_ride_id \
                ( \
                    ride_id VARCHAR(16), \
                    rideable_type VARCHAR(16), \
                    started_at TIMESTAMP_NTZ, \
                    ended_at TIMESTAMP_NTZ, \
                    start_station_name VARCHAR(64), \
                    start_station_id VARCHAR(16), \
                    end_station_name VARCHAR(64), \
                    end_station_id VARCHAR(16), \
                    start_lat VARCHAR(32), \
                    start_lng VARCHAR(32), \
                    end_lat VARCHAR(32), \
                    end_lng VARCHAR(32), \
                    member_casual VARCHAR(16) \
                )").collect()

    session.sql("CREATE OR REPLACE TABLE citibike_db.citibike_sc.citibike_dq_bad_rideable_type \
                ( \
                    ride_id VARCHAR(16), \
                    rideable_type VARCHAR(16), \
                    started_at TIMESTAMP_NTZ, \
                    ended_at TIMESTAMP_NTZ, \
                    start_station_name VARCHAR(64), \
                    start_station_id VARCHAR(16), \
                    end_station_name VARCHAR(64), \
                    end_station_id VARCHAR(16), \
                    start_lat VARCHAR(32), \
                    start_lng VARCHAR(32), \
                    end_lat VARCHAR(32), \
                    end_lng VARCHAR(32), \
                    member_casual VARCHAR(16) \
                )").collect()

    session.sql("CREATE OR REPLACE TABLE citibike_db.citibike_sc.citibike_dq_bad_geospatial \
                ( \
                    ride_id VARCHAR(16), \
                    rideable_type VARCHAR(16), \
                    started_at TIMESTAMP_NTZ, \
                    ended_at TIMESTAMP_NTZ, \
                    start_station_name VARCHAR(64), \
                    start_station_id VARCHAR(16), \
                    end_station_name VARCHAR(64), \
                    end_station_id VARCHAR(16), \
                    start_lat VARCHAR(32), \
                    start_lng VARCHAR(32), \
                    end_lat VARCHAR(32), \
                    end_lng VARCHAR(32), \
                    member_casual VARCHAR(16) \
                )").collect()

    session.sql("CREATE OR REPLACE TABLE citibike_db.citibike_sc.citibike_weather_ny \
                AS \
                SELECT date_valid_std, AVG(avg_temperature_feelslike_2m_f) AS avg_temp \
                FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.standard_tile.history_day \
                WHERE country = 'US' AND postal_code like '100%' \
                GROUP BY date_valid_std \
                ").collect()

    session.sql("CREATE OR REPLACE TABLE citibike_db.citibike_sc.citibike_transformed \
                ( \
                    ride_id VARCHAR(16), \
                    rideable_type VARCHAR(16), \
                    start_station_name VARCHAR(64), \
                    start_station_id VARCHAR(16), \
                    end_station_name VARCHAR(64), \
                    end_station_id VARCHAR(16), \
                    member_casual VARCHAR(16), \
                    start_point GEOGRAPHY, \
                    end_point  GEOGRAPHY, \
                    distance_in_meters NUMBER(8,0), \
                    started_at TIMESTAMP_NTZ, \
                    ended_at TIMESTAMP_NTZ, \
                    elapsed_time_min NUMBER(8,0), \
                    day_name VARCHAR(16), \
                    avg_temp NUMBER(5,2), \
                    trip_period VARCHAR(16) \
                )").collect()








