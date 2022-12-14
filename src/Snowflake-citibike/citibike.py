from citibike_session import make_session
from citibike_create import *
from citibike_load import *
from citibike_stream_task_dq import *
from citibike_transformations import *

# Parameters
accountname = "abcd"
username = "abcd"  
password = "abcd"
rolename = "ACCOUNTADMIN"
stagename= "my_stage"
filepath = "data/input/"
destpath = "data/processed"
tablename = "citibike"

# Main functions
def create_artifacts(session):
    create_virtual_warehouse(session)
    create_database(session)
    create_schema(session)
    create_tables(session)

def create_streams_tasks_dq(session):
    create_streams(session)
    create_task_dq_1(session)
    create_task_dq_bad_nulls(session)
    create_task_dq_bad_ride_id(session)
    create_task_dq_bad_rideable_type(session)
    create_task_dq_bad_geospatial(session)
    create_dag(session)
    activate_tasks(session)

def create_stream_task_transform(session):
    create_stream_transform(session)
    create_task_transform(session)
    activate_task_transform(session)

def load_data(session, filepath, destpath, stagename, tablename):
    stage_created = create_temp_stage(session, stagename)
    upload_file_stage(session, filepath, destpath, stagename, tablename)


# Create session
session = make_session(accountname, username, password, rolename)

# Ask for operation
max = 10
while max > 0:
    max = max -1
    op = input("Operation [create | load | end]:")
    if op == 'create':
        print("Creating artifacts: Virtual Warehouse, Database, Schema, Tables, Streams, Tasks")
        create_artifacts(session)
        create_streams_tasks_dq(session)
        create_stream_task_transform(session)
    elif op == 'load':
        print("Loading data from ", filepath)
        load_data(session, filepath, destpath, stagename, tablename)
    elif op == 'end':
        print('Exiting...')
        exit(0)
    else:
        print('Invalid choice')





