import os
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import *


# Data structures
file_schema = StructType([ \
                            StructField("ride_id", StringType()), \
                            StructField("rideable_type", StringType()), \
                            StructField("started_at", TimestampType()), \
                            StructField("ended_at", TimestampType()), \
                            StructField("start_station_name", StringType()), \
                            StructField("start_station_id", StringType()), \
                            StructField("end_station_name", StringType()), \
                            StructField("end_station_id", StringType()), \
                            StructField("start_lat", StringType()), \
                            StructField("start_lng", StringType()), \
                            StructField("end_lat", StringType()), \
                            StructField("end_lng", StringType()), \
                            StructField("member_casual", StringType()) \
                        ])

# Function definitions
def create_temp_stage(session, stagename):
    session.use_warehouse('citibike_wh')
    session.use_database('citibike_db')
    session.use_schema('citibike_sc')
    stage_created = session.sql(f"CREATE OR REPLACE STAGE {stagename};").collect()
    return stage_created

def upload_file_stage(session, filepath, destpath, stagename, tablename):
    session.use_warehouse('citibike_wh')
    session.use_database('citibike_db')
    session.use_schema('citibike_sc')
    filelist = os.listdir(filepath)
    for f in filelist:
        fullfilename = os.path.join(filepath, f)
        put_result = session.file.put(fullfilename, stagename)
        df1 = session.read.options({"compression": "gzip", "field_delimiter": ",", "skip_header": 1}).schema(file_schema).csv(f"@{stagename}/{f}")
        df1.copy_into_table(tablename,FORCE= True)
        processedfile = os.path.join(destpath, f)
        os.rename(fullfilename, processedfile)









