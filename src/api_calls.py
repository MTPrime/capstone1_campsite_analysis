import requests
import json
import os

def get_campsites(api_key, limit=50, offset=0):

    request_url = 'https://ridb.recreation.gov/api/v1/campsites?limit=' + str(limit) + '&offset=' + str(offset)
    api_data = {
        "accept": "application/json",
        "apikey": api_key
    }

    r = requests.get(request_url, api_data)
    return r.json()

if __name__ =='__main__':
    api_key = os.environ['REC_GOV_KEY']
    print(api_key)
    # test = get_campsites(api_key,1,0)