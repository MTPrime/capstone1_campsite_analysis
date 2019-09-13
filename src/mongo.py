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
    """

    while True:
        try:
            temp_campsite_dict = cursor.next()
        except:
            return df
        
        # attribute_name =['CampsiteID']
        # attribute_value = [temp_campsite_dict['CampsiteID']]
        # print(f"This is the Unstructured data length: {len(temp_campsite_dict[array_name])}")
        
        print(f"CampsiteID: {temp_campsite_dict['CampsiteID']}")
        # print(f"att_name: {att_name}")

        for i in range(len(temp_campsite_dict[array_name])):
            att_name_val = temp_campsite_dict[array_name][i][att_name]
            camp_id = int(temp_campsite_dict['CampsiteID'])

            # print(f"att_name_val: {temp_campsite_dict[array_name][i][att_name]}")
            # print(f"Value?: {temp_campsite_dict[array_name][i][att_val]}")
            df.loc[[camp_id], [att_name_val]] = temp_campsite_dict[array_name][i][att_val]
        
            # attribute_name.append(temp_campsite_dict[array_name][i]['AttributeName'])
            # attribute_value.append(temp_campsite_dict[array_name][i]['AttributeValue'])

        # print(f"Attribute Names: {attribute_name}")
        # print(f"Attribute Values: {attribute_value}")
        # temp_df = pd.DataFrame([attribute_value], columns=attribute_name)

        # temp_df.insert(0, 'CampsiteID', temp_campsite_dict['CampsiteID'], inplace)
        # df_attributes = df_attributes.append(temp_df, sort=True)
        #ignore_index=True

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

        print(f"This is the structured data length: {len(temp_campsite_dict)}")
        temp_df = pd.DataFrame.from_dict(temp_campsite_dict, orient='index').transpose()

        df_campsite_structured = df_campsite_structured.append(temp_df)

if __name__ =='__main__':
    print('Hello')
    api_key = os.environ['REC_GOV_KEY']
    client = MongoClient('localhost', 27017)
    db = client['campsites']
    table = db['test']

    # test = get_campsites(api_key,3, 0)
    # write_api_to_mongodb('campsites', 'test2',api_key=api_key, num_call=1, limit=3)

    
    # cursor2 = db.test.find({"RECDATA.CampsiteID":'70417'})
   
   
    #Getting the unique CampsiteIDs as a list
    # campsite_id_list = db.test.distinct("CampsiteID")
    
    
    # cursor = db.test.find({}, {"CampsiteID":1})
    # cursor = db.test.find({"CampsiteID":"88614"})

    # structured_data_cursor = db.test.find()
    # df_campsite_structured = structured_data_to_panda(structured_data_cursor)
    # df_campsite_structured.to_csv(r'data/campsite_structured.csv', header=True)
    # {"CampsiteID":"66159"}
    
    """Good Code. Doesn't Need to be Run"""
    # #Finds Unique Attributes in order to make columns for DataFrame
    unique_attribute_cursor = db.test.find()
    unique_attributes = find_unique_attributes(unique_attribute_cursor) # array_name='PERMITTEDEQUIPMENT', att_name='EquipmentName')

    # #Finds Unique Campsite IDs to make row for DataFrame
    df_campsite_structured = pd.read_csv('data/campsite_structured.csv')
    structured_ids = set(df_campsite_structured['CampsiteID'])
    
    # #Creates DataFrame of attributes and IDs
    df_attributes_empty = pd.DataFrame(np.nan, index=structured_ids, columns = unique_attributes)

    camp_ids = list(structured_ids)
    for i in range(len(camp_ids)):
        id_string = str(camp_ids[i])
        print(id_string)
        attribute_cursor = db.test.find({"CampsiteID":id_string}) # db.test.find() 
        df_attributes_empty = unstructured_data_to_panda(attribute_cursor, df_attributes_empty) #array_name='PERMITTEDEQUIPMENT', att_name='EquipmentName', att_val='MaxLength'
    
    df_attributes = df_attributes_empty

    
    # #Writing to CSV
    file_string = "data/campsite_attributes.csv"
    df_attributes.to_csv(file_string, header=True, index_label='CampsiteID')
    
    print(datetime.now() - startTime)

    # temp_campsite_dict = cursor.next()
    
    #S3 Connection
    # boto3_connection = boto3.resource('s3')
    # bucket_name = 'mt-capstone1-campsite-analysis'
    # s3_client = boto3.client('s3')

    #Writing to S3
    # s3_client.upload_file('data/campsite_structured.csv', bucket_name, 'campsite_structured.csv')
    # s3_client.upload_file('data/campsite_attributes.csv', bucket_name, 'campsite_attributes.csv')
    

    #Close MongoDB
    client.close()