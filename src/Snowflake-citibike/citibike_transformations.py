
def create_stream_transform(session):
    session.sql("CREATE OR REPLACE STREAM stream_transform ON TABLE citibike_db.citibike_sc.citibike_dq_passed").collect()


def create_task_transform(session):
    session.sql("CREATE OR REPLACE TASK task_transform \
                 WAREHOUSE = citibike_wh \
                 SCHEDULE = '1 minute' \
                 WHEN \
                 SYSTEM$STREAM_HAS_DATA('stream_transform') \
                 AS \
                 INSERT INTO citibike_db.citibike_sc.citibike_transformed \
                 SELECT \
                    c.ride_id, \
                    c.rideable_type, \
                    c.start_station_name, \
                    c.start_station_id, \
                    c.end_station_name, \
                    c.end_station_id, \
                    c.member_casual, \
                    CONCAT('POINT(', c.start_lng, ' ', c.start_lat, ')') as start_point, \
                    CONCAT('POINT(', c.end_lng, ' ', c.end_lat, ')') as end_point, \
                    round(st_distance(st_point(c.start_lng,c.start_lat), st_point(c.end_lng,c.end_lat))) as distance_in_meters, \
                    c.started_at, \
                    c.ended_at, \
                    datediff(minute, c.started_at, c.ended_at) as elapsed_time_min, \
                    dayname(c.started_at) as day_name, \
                    w.avg_temp, \
                    case \
                    when date_part(hour,c.started_at) >=6 and date_part(hour,c.started_at) < 12 then 'Morning' \
                    when date_part(hour,c.started_at) >=12 and date_part(hour,c.started_at) < 18 then 'Afternoon' \
                    when date_part(hour,c.started_at) >=18 or date_part(hour,c.started_at) < 6 then 'Evening' \
                    end as trip_period \
                 from citibike_dq_passed as c inner join citibike_weather_ny as w \
                 on date_trunc('DAY', c.started_at) = w.date_valid_std; \
                ").collect()

def activate_task_transform(session):
    session.sql("alter task task_transform resume;").collect()