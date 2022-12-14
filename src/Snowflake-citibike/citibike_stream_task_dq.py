
def create_streams(session):
    session.sql("CREATE OR REPLACE STREAM stream_dq_1 ON TABLE citibike_db.citibike_sc.citibike").collect()
    session.sql("CREATE OR REPLACE STREAM stream_dq_2 ON TABLE citibike_db.citibike_sc.citibike").collect()
    session.sql("CREATE OR REPLACE STREAM stream_dq_3 ON TABLE citibike_db.citibike_sc.citibike").collect()
    session.sql("CREATE OR REPLACE STREAM stream_dq_4 ON TABLE citibike_db.citibike_sc.citibike").collect()
    session.sql("CREATE OR REPLACE STREAM stream_dq_5 ON TABLE citibike_db.citibike_sc.citibike").collect()


def create_task_dq_1(session):
    session.sql("CREATE OR REPLACE TASK task_dq_1 \
                 WAREHOUSE = citibike_wh \
                 SCHEDULE = '1 minute' \
                 WHEN \
                 SYSTEM$STREAM_HAS_DATA('stream_dq_1') \
                 AS \
                 INSERT INTO citibike_db.citibike_sc.citibike_dq_passed \
                    SELECT ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual \
                    FROM stream_dq_1 \
                    WHERE \
                    METADATA$ACTION = 'INSERT' \
                    AND ride_id IS NOT NULL AND start_station_name IS NOT NULL AND start_station_id IS NOT NULL AND end_station_name IS NOT NULL AND end_station_id IS NOT NULL \
                    AND rideable_type IN ('classic_bike', 'docked_bike', 'electric_bike') \
                    AND start_lat >= -90 AND start_lat <= 90 AND end_lat >= -90 AND end_lat <= 90 \
                    AND start_lng >= -180 AND start_lng <= 180 AND end_lng >= -180 AND end_lng <= 180 \
                    QUALIFY COUNT(ride_id) OVER (PARTITION BY ride_id ORDER BY ride_id) = 1; \
                ").collect()

def create_task_dq_bad_nulls(session):
    session.sql("CREATE OR REPLACE TASK task_dq_bad_nulls \
                 WAREHOUSE = citibike_wh \
                 AS \
                 INSERT INTO citibike_db.citibike_sc.citibike_dq_bad_nulls \
                    SELECT ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual \
                    FROM stream_dq_2 \
                    WHERE \
                    METADATA$ACTION = 'INSERT' \
                    AND ride_id IS NULL OR rideable_type IS NULL OR start_station_name IS NULL OR start_station_id IS NULL OR end_station_name IS NULL OR end_station_id IS NULL; \
                ").collect()

def create_task_dq_bad_ride_id(session):
    session.sql("CREATE OR REPLACE TASK task_dq_bad_ride_id \
                 WAREHOUSE = citibike_wh \
                 AS \
                 INSERT INTO citibike_db.citibike_sc.citibike_dq_bad_ride_id \
                    SELECT ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual \
                    FROM stream_dq_3 \
                    WHERE \
                    METADATA$ACTION = 'INSERT' \
                    QUALIFY COUNT(ride_id) OVER (PARTITION BY ride_id ORDER BY ride_id) > 1; \
                ").collect()

def create_task_dq_bad_rideable_type(session):
    session.sql("CREATE OR REPLACE TASK task_dq_bad_rideable_type \
                 WAREHOUSE = citibike_wh \
                 AS \
                 INSERT INTO citibike_db.citibike_sc.citibike_dq_bad_rideable_type \
                    SELECT ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual \
                    FROM stream_dq_4 \
                    WHERE \
                    METADATA$ACTION = 'INSERT' \
                    AND rideable_type NOT IN ('classic_bike', 'docked_bike', 'electric_bike'); \
                ").collect()

def create_task_dq_bad_geospatial(session):
    session.sql("CREATE OR REPLACE TASK task_dq_bad_geospatial \
                 WAREHOUSE = citibike_wh \
                 AS \
                 INSERT INTO citibike_db.citibike_sc.citibike_dq_bad_geospatial \
                    SELECT ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual \
                    FROM stream_dq_5 \
                    WHERE \
                    METADATA$ACTION = 'INSERT' \
                    AND  start_lat < -90 OR start_lat > 90 OR end_lat < -90 OR end_lat > 90 \
                    AND start_lng < -180 OR start_lng > 180 OR end_lng < -180 OR end_lng > 180; \
                ").collect()

def create_dag(session):
    session.sql("alter task task_dq_bad_nulls add after task_dq_1;").collect() 
    session.sql("alter task task_dq_bad_ride_id add after task_dq_1;").collect()      
    session.sql("alter task task_dq_bad_rideable_type add after task_dq_1;").collect()      
    session.sql("alter task task_dq_bad_geospatial add after task_dq_1;").collect()      

def activate_tasks(session):
    session.sql("alter task task_dq_bad_nulls resume;").collect()
    session.sql("alter task task_dq_bad_ride_id resume;").collect()
    session.sql("alter task task_dq_bad_rideable_type resume;").collect()
    session.sql("alter task task_dq_bad_geospatial resume;").collect()
    session.sql("alter task task_dq_1 resume;").collect()

