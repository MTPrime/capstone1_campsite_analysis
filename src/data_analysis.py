import os,sys
import psycopg2
import pandas as pd 
import tempfile
from psql_pipeline import Pipeline
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np 
import seaborn as sns

plt.style.use('ggplot')
pd.set_option('display.max_columns', 40)

startTime = datetime.now()


def read_sql_tmpfile(query, conn):
    """
    Doing several read_sql queries in a row got very slow. Read that saving to a temporary file might improve speed.
    This is the function to do that.
    """
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
           query=query, head="HEADER"
        )
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
        items = list of tuples - pairs of items to be changed to from. First item is what you want it to be. Second is what it is.
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

def trait_vs_popularity(table, column, conn, where=0):
    """
    Gets the individual values for an attribute column as well as the number of reservations associated with that attribute value.
    Calculates a ratio of reservations to campsites and saves everything to a dataframe.
    
    INPUTS: The where column is there to add the optino for further granularity in the searches.
    
    OUTPUT: A pandas dataframe with trait vs popularity
    """
    if where == 0:
        count_query = "SELECT DISTINCT " + column+ ", COUNT(*) FROM " + table + " GROUP BY " + column
        sum_query = "SELECT DISTINCT " + table + "."+ column + ", sum(res.reservation_count) FROM " + table + " LEFT JOIN reservations as res  ON "+ table +".campsiteid = res.campsite_id WHERE res.reservation_count is not null GROUP BY "+ table+ "."+ column 
    else:
        count_query = "SELECT DISTINCT " + column+ ", COUNT(*) FROM " + table + " WHERE " + column + " = '" + where +"' GROUP BY " + column
        sum_query = "SELECT DISTINCT " + table + "."+ column + ", sum(res.reservation_count) FROM " + table + " LEFT JOIN reservations as res  ON "+ table +".campsiteid = res.campsite_id WHERE res.reservation_count is not null and " + column + " = '" + where +"' GROUP BY "+ table+ "."+ column
    
    count_and_sum = read_sql_tmpfile(count_query, conn)
    sum = read_sql_tmpfile(sum_query, conn)

    count_and_sum['sum'] = sum['sum']
    count_and_sum['ratio'] = count_and_sum['sum']/count_and_sum['count']

    return count_and_sum

def attribute_group_traits(table, traits, conn):
    """
    Used to gather data on all attributes in the "traits" list in order to graph them in a single bar chart.
    """
    df = pd.DataFrame(columns=['trait', 'count', 'sum', 'ratio'])
    
    for i in traits:
        query = "SELECT COUNT(" + table + "."+ i + "), sum(res.reservation_count) FROM " + table + " LEFT JOIN reservations as res  ON "+ table +".campsiteid = res.campsite_id WHERE res.reservation_count is not null and " + table + "." + i + " is not null;"
        df_temp = pd.read_sql(query, conn)
        df_temp['trait'] = i
        df_temp['ratio'] = df_temp['sum']/df_temp['count']
        df = df.append(df_temp, ignore_index=True)
    
    return df

def print_all_attributes(file_name, conn): #pd.read_sql is very inefficient. Need to refactor to use tempfile -return as dictionary
    df_attributes = pd.read_csv(file_name)
    att_list = df_attributes.columns.tolist()
    for i in range(len(att_list)):
        update_messy_columns('attributes', str(att_list[i]), psql_pipeline)
        df= trait_vs_popularity('attributes', str(att_list[i]), conn)
        print(df)

def bar_plot(ax, trait='trait', conn=0, avg=29, use_df=0, df=0):
    if use_df==0:
        df = trait_vs_popularity('attributes', trait, conn)
        #Drops the NaN values and replaces them with the overall average instead.
        df.dropna(inplace=True)
    
    df=df.append({trait:'AVG', 'ratio': avg}, ignore_index=True)
    df = df.sort_values('ratio')
    mask =df.where(df[trait]!= 'AVG').isnull()
    tickLocations = np.arange(len(df))
    
    ax.bar(tickLocations, df['ratio'], .5, edgecolor='black', linewidth=.75, color=(mask['ratio'].map({True:'#CC2529', False:'#396AB1'})))
    ax.set_xticks(ticks = tickLocations)
    ax.set_xticklabels(df[trait], rotation = 45, ha='right')
    ax.set_title(trait)

    return ax
    
def multi_plotting(trait_list,conn, avg=29):
    """
    Meant for 8 traits to make a single 2x4 comparison image
    """
    fig, axs = plt.subplots(2, 4, figsize=(20,20), constrained_layout=True)

    for i, ax in enumerate(axs.flatten()):
        bar_plot(ax, trait_list[i], conn, avg)
    
    plt.savefig("img/traits_vs_popularity.png")
    

if __name__ == '__main__':

    conn = psycopg2.connect(database="campsites", user="postgres", host="localhost", port="5435")
    print("connected")
    psql_pipeline = Pipeline(conn)

    ##Reads in all attributes and prints out their popularity. Used once for general exploratoration. Very slow
    # file_name='data/campsite_attributes_clean.csv'
    # print_all_attributes(file_name, conn)
   
    #Updating columns
    site_rating_list = [('prefer', 'preferred'), ('standard', 'standar')]
    update_messy_columns('attributes', 'site_rating', psql_pipeline, site_rating_list)
    #df_site_rating = trait_vs_popularity('attributes', 'site_rating', conn)
    
    prox_water_list = [('island','island,'), ('lakefront', 'lakefront,riverfront')] #Checking the few lakefront,riverfront values showed they came from the same several facilities - all on lakes
    update_messy_columns('attributes', 'proximity_to_water', psql_pipeline, prox_water_list)
    #df_prox_water = trait_vs_popularity('attributes', 'proximity_to_water', conn)

    pets_list =[('horse', 'domestic,horse'), ('yes', 'pets allowed')]
    update_messy_columns('attributes', 'pets_allowed', psql_pipeline, pets_list)
    # #df_pets_allowed = trait_vs_popularity('attributes', 'pets_allowed', conn)

    update_messy_columns('attributes', 'drinking_water', psql_pipeline)
    # # df_drinking_water = trait_vs_popularity('attributes', 'drinking_water', conn)

    update_messy_columns('attributes', 'condition_rating', psql_pipeline)
    # df_condition_rating= trait_vs_popularity('attributes', 'condition_rating', conn)

    update_messy_columns('attributes', 'host', psql_pipeline)
    # # df_host= trait_vs_popularity('attributes', 'host')

    update_messy_columns('attributes', 'flush_toilets', psql_pipeline)
    # # df_flush_toilets= trait_vs_popularity('attributes', 'flush_toilets', conn)

    update_messy_columns('attributes', 'showers', psql_pipeline)
    # # df_showers= trait_vs_popularity('attributes', 'showers')

    update_messy_columns('attributes', 'drinking_water', psql_pipeline)
    # # df_drinking_water= trait_vs_popularity('attributes', 'drinking_water', conn)


    sewer_hookup_list = [('yes', 'sewer hookup'), ('yes','y')]
    update_messy_columns('attributes', 'sewer_hookup', psql_pipeline, sewer_hookup_list)
    #df_sewer_hookup= trait_vs_popularity('attributes', 'sewer_hookup', conn)

    picnic_tables_list = [('yes', 'picnic table'), ('yes','y'), ('yes', 'picnic tables'), ('yes', 'tables'), ('yes', 'table & benches')]
    update_messy_columns('attributes', 'picnic_tables', psql_pipeline, picnic_tables_list)
    #df_picnic_tables= trait_vs_popularity('attributes', 'picnic_tables', conn)


    #Gets the average number of reservations per campsite where campsites have at least 1 reservation.
    average_res = pd.read_sql("SELECT att.campsiteid, sum(res.reservation_count) FROM attributes as att LEFT JOIN reservations as res  ON att.campsiteid = res.campsite_id WHERE res.reservation_count is not null GROUP BY att.campsiteid;",conn)
    average_number= average_res['sum'].sum() / average_res['campsiteid'].count()

    #Graphs 8 of the more interesting traits
    traits=['site_rating', 'proximity_to_water', 'pets_allowed', 'picnic_tables', 'drinking_water', 'sewer_hookup', 'flush_toilets', 'campfire_allowed'] #'host'
    multi_plotting(traits, conn, avg=average_number)

    #Graphs the 12 most popular traits against the average on one chart
    traits=['site_rating', 'proximity_to_water', 'pets_allowed', 'host', 'drinking_water', 'sewer_hookup', 'flush_toilets', 'campfire_allowed', 'food_storage_locker', 'bbq', 'dump_station', 'picnic_tables']
    group_att = attribute_group_traits('attributes', traits, conn)

    fig, ax = plt.subplots(figsize=(10,10))
    bar_plot(ax, conn, avg=average_number, use_df=1, df=group_att)

    plt.savefig("img/traits_ranked_by_popularity.png")


    plt.show()

    print(datetime.now() - startTime)