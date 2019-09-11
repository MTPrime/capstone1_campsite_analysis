import os,sys
import psycopg2
import pandas as pd 
from psql_pipeline import Pipeline
from datetime import datetime
startTime = datetime.now()

def create_table_string(cols, table_name):
    table_string = "CREATE TABLE " + table_name + "( "
    for k, i in enumerate(cols):
        if k == len(cols)-1:
            table_string += i + " character varying(50)"
        else:
            table_string += i + " character varying(50), "
    table_string += ");"
    return table_string

def create_populate_string(cols, table_name, file):
    table_string = "COPY " + table_name + "( "
    for k, i in enumerate(cols):
        if k == len(cols)-1:
            table_string += i
        else:
            table_string += i + ", "
    table_string += ") FROM " + file + " DELIMITER ',' CSV HEADER;"
    return table_string

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


  
    psql_pipeline.execute()
    psql_pipeline.close()                  
