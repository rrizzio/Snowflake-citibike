A practical example of using the Snowflake platform to tackle many data analysis themes.

Dataset:  https://ride.citibikenyc.com/system-data

Procedure:

1- Copy the dataset files ( 2022*-citibike-tripdata.csv ) into data/input

2- Insert your Snowflake credentials in the file src/Snowflake-citibike/citibike.py

3- Execute the main program
src/Snowflake-citibike/citibike.py

4- At the operation prompt, first choose "create" to build all the artifacts

Operation [create | load | end]: create

5- At the operation prompt, then choose "load" to process all files in data/input

Operation [create | load | end]: load

6- The processed files are moved into data/processed

7- End the main program

Operation [create | load | end]: end
