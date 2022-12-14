A practical example of using the Snowflake platform to tackle many data analysis themes.
Dataset:  https://ride.citibikenyc.com/system-data

Procedure:
1- Copy the dataset files ( *-citibike-tripdata.csv ) into data/input

2- Execute the main program
src/my_panel_package/citibike.py

3- At the operation prompt, first choose "create" to build all the artifacts
Operation [create | load | end]: create

4- At the operation prompt, then choose "load" to process all files in data/input
Operation [create | load | end]: load

5- The processed files are moved into data/processed

6- End the main program
Operation [create | load | end]: end
