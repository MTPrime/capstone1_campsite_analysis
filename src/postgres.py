import os,sys
import psycopg2
import pandas as pd 
from psql_pipeline import Pipeline
from datetime import datetime
startTime = datetime.now()

def create_table_string(cols, table_name):
    """
    Takes a list of column names and generates an SQL query to make a new table. Useful because there are a lot of attribute
    columns and the number read in changes depending on what threshold of NA values are allowed earlier in the cleaning process.

    Note - reads in everything as a varing character in order to avoid 
    type errors with the uncleaned data. Schema can/should be updated later

    INPUTS: 
        cols: list - column names from the read in csv
        table_name: str - name of table to be created
    
    OUTPUTS:
        str: SQL query for creating a new table based on the columns passed in.
    
    """

    table_string = "CREATE TABLE " + table_name + "( "
    for k, i in enumerate(cols):
        if k == len(cols)-1:
            table_string += i + " character varying(50)"
        else:
            table_string += i + " character varying(50), "
    table_string += ");"
    return table_string

def create_populate_string(cols, table_name, file):
    """
    INPUTS: 
        cols: list - column names from the read in csv
        table_name: str - name of table to be created
        file: CSV file
    OUTPUTS:
        str: SQL query for importing data from file into a table based on the columns passed in.
    """
    table_string = "COPY " + table_name + "( "
    for k, i in enumerate(cols):
        if k == len(cols)-1:
            table_string += i
        else:
            table_string += i + ", "
    table_string += ") FROM " + file + " DELIMITER ',' CSV HEADER;"
    return table_string

def combine_columns(table, keep_column, moving_column, psql_pipeline):
    combine_query = "UPDATE  " + table + " SET " + keep_column + " = " + moving_column + " WHERE "+ keep_column + " is null;"
    psql_pipeline.add_step(combine_query)





if __name__ =='__main__':
### connect to the database
    conn = psycopg2.connect(database="campsites", user="postgres", host="localhost", port="5435")
    print("connected")

    psql_pipeline = Pipeline(conn)

###Reading in Data
    #Attributes
    df_attributes = pd.read_csv('data/campsite_attributes_clean.csv')
    att_cols = df_attributes.columns.tolist()
    
    #Permitted Equipment
    df_permitted = pd.read_csv('data/campsite_permitted_equipment_clean.csv')
    equip_cols = df_permitted.columns.tolist()
    
    #Dropping Tables if they Exist - Note, I tried drop them all as comma seperated values. Combined with the If Exists call it froze. Seperated out instead.
    tables_list = ['campsites','reservations','equipment','attributes']
    for i in tables_list:
        string = "DROP TABLE IF EXISTS " + i + ";"
        psql_pipeline.add_step(string)

    #Campsites Queries
    campsite_create_query = """CREATE TABLE campsites
                            (
                                zero character varying(10),
                                _id character varying(50),
                                CampsiteID character varying(50),
                                FacilityID character varying(50),
                                CampsiteName character varying(50),
                                CampsiteType character varying(50),
                                TypeOfUse character varying(50),
                                Loop character varying(100),
                                CampsiteAccessible BOOLEAN,
                                CampsiteLongitude numeric,
                                CampsiteLatitude numeric,
                                CreatedDate DATE,
                                LastUpdatedDate DATE
                            );
                            """
    

    campsite_populate_query = """ 
                                COPY campsites(
                                    zero,
                                    _id, 
                                    CampsiteID, 
                                    FacilityID, 
                                    CampsiteName, 
                                    CampsiteType, 
                                    TypeOfUse, 
                                    Loop, 
                                    CampsiteAccessible, 
                                    CampsiteLongitude, 
                                    CampsiteLatitude, 
                                    CreatedDate, 
                                    LastUpdatedDate) 
                                FROM '/home/data/Galvanize/capstones/capstone1_campsite_analysis/data/campsite_structured.csv' DELIMITER ',' CSV HEADER;
                                """
    
    # Structured Data Query and Commit
    psql_pipeline.add_step(campsite_create_query)
    psql_pipeline.add_step(campsite_populate_query)
    

    #Permitted Equipment Query and Commit
    file = "'/home/data/Galvanize/capstones/capstone1_campsite_analysis/data/campsite_permitted_equipment.csv'"
    psql_pipeline.add_step(create_table_string(equip_cols, 'equipment'))
    psql_pipeline.add_step(create_populate_string(equip_cols, 'equipment', file))
    

    #Attributes Query and Commit
    file = "'/home/data/Galvanize/capstones/capstone1_campsite_analysis/data/campsite_attributes_clean.csv'"
    psql_pipeline.add_step(create_table_string(att_cols, 'attributes'))
    psql_pipeline.add_step(create_populate_string(att_cols, 'attributes', file))

    #Reservation Counts
    create_reservation_count_query = """CREATE TABLE reservations 
                                        (
                                            campsite_id character varying(25),
                                            reservation_count int
                                        );"""
    populate_reservation_count_query = """COPY reservations 
                                            (
                                            campsite_id,
                                            reservation_count 
                                            )
                                        FROM '/home/data/Galvanize/capstones/capstone1_campsite_analysis/data/campsite_reservation_count.csv' DELIMITER ',' CSV HEADER;"""
    
    psql_pipeline.add_step(create_reservation_count_query)
    psql_pipeline.add_step(populate_reservation_count_query)


    #Deleting campsites with 0,0 lat/long
    # lat_long_del = """DELETE FROM attributes 
    #                 WHERE attributes.campsiteid 
    #                 in 
    #                     (SELECT camp.campsiteid 
    #                     FROM campsites as camp 
    #                     WHERE campsitelongitude = 0);"""
    
    #Deleting campsites created after reservation data
    created_date_del = """DELETE FROM attributes 
                        WHERE attributes.campsiteid 
                        in 
                            (SELECT camp.campsiteid 
                            FROM campsites as camp 
                            WHERE createddate::date >= '2019-01-01'::date);"""
    psql_pipeline.add_step(created_date_del)

#General Combining 
    
    #Condition Rating/Site Rating
    combine_columns('attributes', 'site_rating', 'condition_rating', psql_pipeline)
    
    #Toilets
    combine_columns('attributes', 'flush_toilets', 'vault_toilets', psql_pipeline)
    combine_columns('attributes', 'flush_toilets', 'accessible_vault_toilets', psql_pipeline)

    #BBQ
    combine_columns('attributes', 'bbq', 'grills', psql_pipeline)

    #Showers
    combine_columns('attributes', 'showers', 'accessible_showers', psql_pipeline)
    # combine_columns('attributes', 'showers', 'shower_bath_type', psql_pipeline)

    #Water Hookups
    combine_columns('attributes', 'water_hookup', 'water_hookups', psql_pipeline)

    #Tables
    combine_columns('attributes', 'picnic_tables', 'picnic_table', psql_pipeline)
    combine_columns('attributes', 'picnic_tables', 'table_and_benches', psql_pipeline)
    combine_columns('attributes', 'picnic_tables', 'tables', psql_pipeline)


    
    psql_pipeline.execute()
    psql_pipeline.close()                  
    print(datetime.now() - startTime)