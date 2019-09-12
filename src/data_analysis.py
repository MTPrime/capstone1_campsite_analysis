import os,sys
import psycopg2
import pandas as pd 
import tempfile
from psql_pipeline import Pipeline
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np 
plt.style.use('ggplot')
pd.set_option('display.max_columns', 40)

startTime = datetime.now()


def read_sql_tmpfile(query, conn):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
           query=query, head="HEADER"
        )
        # conn = db_engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile)
        return df

def update_messy_columns(table, column, psql_pipeline, items=0):
    """
    INPUTS:
        table = str - the table to be updated
        column = str - column to be updated
        items = list of tuples - pairs of items to be changed from to
           ex: [('prefer', 'preferred'), ('Primest', 'prime')]
    OUTPUTS;
        None - just updates the column
    """

    lower_query_string = "UPDATE " + table +" SET " + column + "=lower(" + column +");"
    psql_pipeline.add_step(lower_query_string)
    if items !=0:
        for i in items:
            transformation_query_string = "UPDATE " + table + " SET " + column + "= '" + i[0] + "' WHERE " + column + "= '" + i[1] + "';"
            psql_pipeline.add_step(transformation_query_string)
    
    psql_pipeline.execute()
    print('Updated')
    # initial_query_string = "SELECT DISTINCT" + column + " FROM "+ table +";"

def trait_vs_popularity(table, column, conn=0, where=0):
    """
    OUTPUT: A pandas dataframe with trait vs popularity
    """
    if where == 0:
        count_query = "SELECT DISTINCT " + column+ ", COUNT(*) FROM " + table + " GROUP BY " + column #+ ";"
        sum_query = "SELECT DISTINCT " + table + "."+ column + ", sum(res.reservation_count) FROM " + table + " LEFT JOIN reservations as res  ON "+ table +".campsiteid = res.campsite_id WHERE res.reservation_count is not null GROUP BY "+ table+ "."+ column #+";"
    else:
        count_query = "SELECT DISTINCT " + column+ ", COUNT(*) FROM " + table + " WHERE " + column + " = '" + where +"' GROUP BY " + column + ";"
        sum_query = "SELECT DISTINCT " + table + "."+ column + ", sum(res.reservation_count) FROM " + table + " LEFT JOIN reservations as res  ON "+ table +".campsiteid = res.campsite_id WHERE res.reservation_count is not null and " + column + " = '" + where +"' GROUP BY "+ table+ "."+ column +";"
    
    # count_and_sum = pd.read_sql(count_query, conn)
    # sum = pd.read_sql(sum_query, conn)
    
    count_and_sum = read_sql_tmpfile(count_query, conn)
    sum = read_sql_tmpfile(sum_query, conn)

    count_and_sum['sum'] = sum['sum']
    count_and_sum['ratio'] = count_and_sum['sum']/count_and_sum['count']

    return count_and_sum

def print_all_attributes(file_name): #pd.read_sql is very inefficient. Need to refactor to use I/O String
    df_attributes = pd.read_csv(file_name)
    att_list = df_attributes.columns.tolist()
    for i in range(len(att_list)):
        update_messy_columns('attributes', str(att_list[i]), psql_pipeline)
        df= trait_vs_popularity('attributes', str(att_list[i]))
        print(df)
    
if __name__ == '__main__':

    conn = psycopg2.connect(database="campsites", user="postgres", host="localhost", port="5435")
    print("connected")
    psql_pipeline = Pipeline(conn)

###Reads in all attributes and prints out their popularity. Used once for general exploratoration
    # file_name='data/campsite_attributes_clean.csv'
    # print_all_attributes(file_name)

###Reservations by type
    # reservations_by_type = """SELECT DISTINCT campsitetype, COUNT(*) as res_count from campsites GROUP BY campsitetype ORDER BY COUNT(*) DESC LIMIT 10;"""
    
    # data = pd.read_sql(reservations_by_type, conn)
    
    
    # #Plotting the Bar Graph of Res Count by Type
    # fig, ax = plt.subplots(figsize=(12,6))
    # tickLocations = np.arange(len(data))
    # ax.bar(tickLocations, data['res_count'], .8)
    # ax.set_xticks(ticks = tickLocations)
    # ax.set_xticklabels(data['campsitetype'], rotation = 45, ha='right')
    # ax.set_title("Number of Reservations by Campsite Type")
    # plt.tight_layout(pad=1)
    # plt.show()

    # print(data)
    
    # reservations_by_type = """SELECT DISTINCT campsitetype, COUNT(*) as res_count from campsites GROUP BY campsitetype ORDER BY COUNT(*) DESC LIMIT 10;"""    
    # data = pd.read_sql(reservations_by_type, conn)

#Site Rating Vs Popularity - Great data
    # site_rating_list = [('prefer', 'preferred'), ('standard', 'standar')]
    # update_messy_columns('attributes', 'site_rating', psql_pipeline, site_rating_list)
    # df_site_rating = trait_vs_popularity('attributes', 'site_rating')

#Proximity to Water Vs Popularity -Great Data
    # prox_water_list = [('island','island,'), ('lakefront', 'lakefront,riverfront')] #Checking the few lakefront,riverfront values showed they came from the same several facilities - all on lakes
    # update_messy_columns('attributes', 'proximity_to_water', prox_water_list, psql_pipeline)
    # df_prox_water = trait_vs_popularity('attributes', 'proximity_to_water')

#Finding the number of reservations and campsite ids with at least one reservation
    # data = pd.read_sql('SELECT att.campsiteid, sum(res.reservation_count) FROM attributes as att LEFT JOIN reservations as res  ON att.campsiteid = res.campsite_id WHERE res.reservation_count is not null GROUP BY att.campsiteid;' , conn)

#Boat Dock - boat_dock
    # update_messy_columns('attributes', 'boat_dock', prox_water_list, psql_pipeline)
    # df_boat_dock = trait_vs_popularity('attributes', 'boat_dock')

#Lake Access - lake_access
    # update_messy_columns('attributes', 'lake_access', prox_water_list, psql_pipeline)
    # df_lake_access = trait_vs_popularity('attributes', 'lake_access')

"""
#hike_in_distance_to_site  -Super Messy
    # update_messy_columns('attributes', 'hike_in_distance_to_site', prox_water_list, psql_pipeline)
    # df_hike_in_distance_to_site = trait_vs_popularity('attributes', 'hike_in_distance_to_site')

#max_num_of_people - Messy, needs cleaning by casting to int
    # update_messy_columns('attributes', 'max_num_of_people', prox_water_list, psql_pipeline)
    # df_max_num_of_people = trait_vs_popularity('attributes', 'max_num_of_people')
"""
#pets_allowed - Horses Negative, explore income
#     pets_list =[('horse', 'domestic,horse'), ('yes', 'pets allowed')]
#     update_messy_columns('attributes', 'pets_allowed', pets_list, psql_pipeline)
#     df_pets_allowed = trait_vs_popularity('attributes', 'pets_allowed')

# #drinking_water - High
#     update_messy_columns('attributes', 'drinking_water', pets_list, psql_pipeline)
#     df_drinking_water = trait_vs_popularity('attributes', 'drinking_water')

# # site_access
#     site_access_list = [('drive-in', 'drive in'), ('hike-in_drive-in','hike-in,drive-in')]
#     update_messy_columns('attributes', 'site_access', site_access_list, psql_pipeline)
#     df_site_access = trait_vs_popularity('attributes', 'site_access')

"""
# #platform - I don't know what this one means
#     update_messy_columns('attributes', 'platform', site_access_list, psql_pipeline)
#     df_platform= trait_vs_popularity('attributes', 'platform')
"""

#condition_rating
    # update_messy_columns('attributes', 'condition_rating', site_access_list, psql_pipeline)
    # df_condition_rating= trait_vs_popularity('attributes', 'condition_rating')

# #shade
#     shade_list = [('shade', 'shade '), ('yes', 'shade')]
#     update_messy_columns('attributes', 'shade', psql_pipeline, shade_list)
#     df_shade= trait_vs_popularity('attributes', 'shade')

#host - High
    # update_messy_columns('attributes', 'host', psql_pipeline)
    # df_host= trait_vs_popularity('attributes', 'host')

#flush_toilets
# update_messy_columns('attributes', 'flush_toilets', psql_pipeline)
# df_flush_toilets= trait_vs_popularity('attributes', 'flush_toilets')

#showers
# update_messy_columns('attributes', 'showers', psql_pipeline)
# df_showers= trait_vs_popularity('attributes', 'showers')

#drinking_water
# update_messy_columns('attributes', 'drinking_water', psql_pipeline)
# df_drinking_water= trait_vs_popularity('attributes', 'drinking_water')

#lean_to_shelter
# lean_to_list = [('yes', 'lean to/shelter'), ('yes','y')]
# update_messy_columns('attributes', 'lean_to_shelter', psql_pipeline, lean_to_list)
# df_lean_to_shelter= trait_vs_popularity('attributes', 'lean_to_shelter')

# #sewer_hookup
# sewer_hookup_list = [('yes', 'sewer hookup'), ('yes','y')]
# update_messy_columns('attributes', 'sewer_hookup', psql_pipeline, sewer_hookup_list)
# df_sewer_hookup= trait_vs_popularity('attributes', 'sewer_hookup')

#YURT!!!
# update_messy_columns('campsites', 'campsitetype', psql_pipeline)
# df_yurt= trait_vs_popularity('campsites', 'campsitetype', where='yurt')

# #campsitetype
# update_messy_columns('campsites', 'campsitetype', psql_pipeline)
# df_campsite_type= trait_vs_popularity('campsites', 'campsitetype')

df_horses= trait_vs_popularity('equipment', 'horse', conn)

print(datetime.now() - startTime)
