import pandas as pd 
# df = pd.read_csv('data/reservations2018.csv', encoding='latin-1')

df_attributes = pd.read_csv('data/campsite_attributes.csv')
df_campsite_structured = pd.read_csv('data/campsite_structured.csv')
df_equipment = pd.read_csv('data/campsite_permitted_equipment.csv')
df_reservations = pd.read_csv('data/campsite_reservation_count.csv')

#Trouble Shooting to see why certain campsites were not written
# attribute_ids = set(df_attributes['CampsiteID'])
# structured_ids = set(df_campsite_structured['CampsiteID'])


def cleaning_column_names(df, file_to_write, threshold=0):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('-', '_').str.replace('/', '_').str.replace('&', 'and').str.replace("`", "")
    df.dropna(axis='columns', thresh=threshold, inplace=True)
    df.to_csv(file_to_write, header=True, index=False)


#Cleaning Attributes
# file_to_write = 'data/campsite_attributes_clean.csv'
# cleaning_column_names(df_attributes, file_to_write, threshold=250)

#Cleaning Equipment
# file_to_write = 'data/campsite_permitted_equipment_clean.csv'
# cleaning_column_names(df_equipment, file_to_write)

#Cleaning Campsites
# file_to_write = 'data/campsite_structured_clean.csv'
# cleaning_column_names(df_campsite_structured, file_to_write)