from pymongo import MongoClient
from api_calls import get_campsites
import pandas as pd 
import numpy as np 
import os 
import boto3
from datetime import datetime
startTime = datetime.now()

def write_api_to_mongodb(db, table, api_key, client=MongoClient('localhost', 27017), num_call=0, limit=50):
    
    #Instantiates Connection
    client = client
    db = client[db]
    table = db[table]
    
    #Loops through the API calls at the largest chunksize available. (50)
    for i in range(num_call):
        campsite_chunk = get_campsites(api_key,limit, (i*50))
        table.insert_many(campsite_chunk['RECDATA'])
    
    client.close()

def find_unique_attributes(cursor, array_name='ATTRIBUTES', att_name='AttributeName'):
    """
    Loops through the database to find unique attribute names. These will become
    the columns in the PSQL database.
    
    Inputs
    cursor: mongoDB cursor object with a query of campsites from the database.

    Output
    attribute_set: A set of unique attributes
    """
    attribute_set = set()
    while True:
        try:
            temp_campsite_dict = cursor.next()
        except:
            return attribute_set 

        for i in range(len(temp_campsite_dict[array_name])):
            attribute_set.add(temp_campsite_dict[array_name][i][att_name])
    
def unstructured_data_to_panda(cursor, df, array_name = 'ATTRIBUTES', att_name='AttributeName', att_val='AttributeValue'):
    """
    Attributes, media information, and permitted equipment come in as arrays with variable key values.
    This function converts each campsite in the MongoDB into a row in a Pandas dataframe where the 
    columns are the keys in those arrays. Tested and works for both Attributes and Permitted Equipment.

    INPUT:
        cursor - cursor object. Each call will be a new campsite
        df - dataframe of unique campsite id's as the row and unique attribute names as the columns. NaN values. Roughly (76,000, 300)
        array_name, att_name, att_val - str Used to target values based on the json schema from the API.
    OUTPUT:
        df - Same dataframe, but with values replacing NaN.
    """

    while True:
        try:
            temp_campsite_dict = cursor.next()
        except:
            return df
        
        print(f"CampsiteID: {temp_campsite_dict['CampsiteID']}")

        #Loops though all items in the cursor. Identifies the attribute name (att_name_val) Saves to the data frame at the row, column that is appropriate.
        for i in range(len(temp_campsite_dict[array_name])):
            att_name_val = temp_campsite_dict[array_name][i][att_name]
            camp_id = int(temp_campsite_dict['CampsiteID'])
            df.loc[[camp_id], [att_name_val]] = temp_campsite_dict[array_name][i][att_val]
        

def structured_data_to_panda(cursor):
    """
    Some of the data from the API call is in a consistent structured form. This function converts each campsite
    in the MongoDB into a row in a Pandas dataframe where the columns are the key values.
    """

    df_campsite_structured = pd.DataFrame()
    
    #The following keys come in as arrays with inconsistent key values. Removing them to get the core data in easier
    entriesToRemove = ('ATTRIBUTES', 'ENTITYMEDIA', 'PERMITTEDEQUIPMENT')
    while True:
        try:
            temp_campsite_dict = cursor.next()
            
            for k in entriesToRemove:
                temp_campsite_dict.pop(k, None)
        except:
            return df_campsite_structured 

        temp_df = pd.DataFrame.from_dict(temp_campsite_dict, orient='index').transpose()

        df_campsite_structured = df_campsite_structured.append(temp_df)

if __name__ =='__main__':
    api_key = os.environ['REC_GOV_KEY']
    client = MongoClient('localhost', 27017)
    db = client['campsites']
    table = db['test']

    #Making API Calls and writing to MongoDB
    # write_api_to_mongodb('campsites', 'test',api_key=api_key, num_call=1537)

    
    #Pulling the structured campsite data out from MongoBD and putting it into a CSV
    structured_data_cursor = db.test.find()
    df_campsite_structured = structured_data_to_panda(structured_data_cursor)
    df_campsite_structured.to_csv(r'data/campsite_structured.csv', header=True)
    
    
    # Finds Unique Attributes in order to make columns for DataFrame
    unique_attribute_cursor = db.test.find()
    unique_attributes = find_unique_attributes(unique_attribute_cursor) # array_name='PERMITTEDEQUIPMENT', att_name='EquipmentName')

    # Finds Unique Campsite IDs to make row for DataFrame
    df_campsite_structured = pd.read_csv('data/campsite_structured.csv')
    structured_ids = set(df_campsite_structured['CampsiteID'])
    
    # #Creates DataFrame of attributes and IDs
    df_attributes_empty = pd.DataFrame(np.nan, index=structured_ids, columns = unique_attributes)

   

    #Calls a individual search for each campsite id and gets the attributes from the results. 
    #Originally made one call and used the cursor to go through each campsite. Data was lost in the process. 
    # This method was slower, but retained all data.
    
    camp_ids = list(structured_ids)
    for i in range(len(camp_ids)):
        id_string = str(camp_ids[i])
        attribute_cursor = db.test.find({"CampsiteID":id_string}) 
        df_attributes_empty = unstructured_data_to_panda(attribute_cursor, df_attributes_empty)
    
    df_attributes = df_attributes_empty

    
    # #Writing to CSV
    file_string = "data/campsite_attributes.csv"
    df_attributes.to_csv(file_string, header=True, index_label='CampsiteID')
    
    print(datetime.now() - startTime)

    
    #S3 Connection
    # boto3_connection = boto3.resource('s3')
    # bucket_name = 'mt-capstone1-campsite-analysis'
    # s3_client = boto3.client('s3')

    #Writing to S3
    # s3_client.upload_file('data/campsite_structured.csv', bucket_name, 'campsite_structured.csv')
    # s3_client.upload_file('data/campsite_attributes.csv', bucket_name, 'campsite_attributes.csv')
    

    #Close MongoDB
    client.close()