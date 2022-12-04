import requests
import json
import pandas as pd
import math
import datetime
from google.cloud import storage
from flask import jsonify
import os
#from dotenv import load_dotenv ## for local debugging, comment out when deploy to gcf

#load_dotenv() ## for local debugging, comment out when deploy to gcf
def ingest_data_from_centanet_to_gcs(request):

    # Initialize GCF API Response
    output_json = {}

    # Initialize function for executing API
    def get_response_batch(url: str, response_size: int, offset: int) -> str:
        """Calls API and returns response json string."""

        request_body = {"postType": "Both",
                        "day": "Day30",
                        "sort": "InsOrRegDate",
                        "order": "Descending",
                        "size": response_size,
                        "offset": "{0}".format(offset),
                        "pageSource": "search"
                        }
        request_headers = {"Lang": "en",
                           "Content-Type":
                           "application/json",
                           "Accept": "gzip, deflate, br"
                           }
        response = requests.post(url, data=json.dumps(
            request_body), headers=request_headers)
        return response

    def upload_blob(bucket_name: str, file_name: str, file_str: str) -> None:
        """Initialize GCS Client and uploads blob to GCS """
        client_GCS = storage.Client()
        client_bucket = client_GCS.get_bucket(bucket_name)
        client_bucket.blob(file_name).upload_from_string(
            file_str)

    # Loop Count
    var_loop_max_size = 0

    # Initialize dataframe
    df_clean = pd.DataFrame()

    # Initialize variables
    api_url = os.environ.get('CENTANET_API_URL') #"https://hk.centanet.com/findproperty/api/Transaction/Search"
    var_api_response_size = int(os.environ.get('API_BATCH_SIZE'))

    # Get Loop value i.e. count of results/batch size
    try:
        row_Count = get_response_batch(
            api_url, var_api_response_size, 0).json().get("count", 0)
    except Exception as e:
        output_json['Error'] = True
        output_json['Failed Stage'] = 'Get count failed'
        output_json['Error Message'] = str(e)

    var_loop_max_size = math.ceil(row_Count/var_api_response_size)

    # Loop
    var_loop = 0
    while var_loop < var_loop_max_size:
        var_offset = var_loop * var_api_response_size
        try:
            df_raw = pd.DataFrame(
            [get_response_batch(api_url, var_api_response_size, var_offset).json()])
        except Exception as e:
                output_json['Error'] = True
                output_json['Failed Stage'] = 'API call'
                output_json['Error Message'] = str(e)
        try:
            df_transform = df_raw.explode('data')[['data']]
            df_transform = df_transform['data'].apply(pd.Series)
            df_clean = pd.concat([df_clean, df_transform])
            var_loop += 1
        except Exception as e:
            output_json['Error'] = True
            output_json['Failed Stage'] = 'Explode'
            output_json['Error Message'] = str(e)
    output_json['Loop count'] = var_loop
    output_json['Row count'] = row_Count

    ## WRITE TO GOOGLE CLOUD STORAGE project-diver-{env}##
    var_bucket_name = os.environ.get('DESTINATION_BUCKET_NAME')
    var_year = str(datetime.datetime.utcnow().strftime('%Y'))
    var_datetime = str(datetime.datetime.utcnow().strftime('%d%m%y'))
    var_file_name = 'STAGING/centanet/{0}/centanet_property_transaction_{1}.parquet'.format(var_year,var_datetime)
    try:
        ## to cloud
        upload_blob(var_bucket_name, var_file_name, df_clean.to_parquet())
        ## to local
        # df_clean.to_parquet("DATASETS/"+var_file_name)
        output_json['Error'] = False
        output_json['Loop'] = 'Succeeded'
    except Exception as e:
        output_json['Error'] = True
        output_json['Failed Stage'] = 'Write'
        output_json['Error Message'] = str(e)

    return jsonify(output_json)
