import pandas as pd
import datetime
from google.cloud import storage
from google.cloud import bigquery
from flask import jsonify
import os
import warnings

warnings.filterwarnings("ignore")

def etl_centanet_data_from_gcs_to_bcq(event,context):
    """Triggered by a change to a Cloud Storage bucket.
        Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
        """
    
    # Initialize GCF API Response
    output_json = {}

    #Initialize functions

    def load_data_to_table(project_name: str, table_name: str, dataframe_clean: pd.DataFrame) -> str:
    ##Directly loads data to table##
    # Construct a BigQuery client object.
        client = bigquery.Client()

        table_id = ".".join([project_name, table_name])

        # job_config = bigquery.LoadJobConfig()
        # job_config.autodetect = True

        load_job = client.load_table_from_dataframe(
            dataframe=dataframe_clean, destination=table_id
        )  # Make an API request.
        
        result = load_job.result()  # Waits for the job to complete.
        return result
        
    def query_data_from_table(project_name: str, dataset_name: str, table_name: str, query_string: str) -> pd.DataFrame:
        client = bigquery.Client()
        query_job = client.query(
            query=query_string,
        )  # API Request
        result = query_job.to_dataframe()# Make the request
        return result

    def upload_blob(bucket_name: str, file_name: str, file_str: str) -> None:
        client_GCS = storage.Client()
        client_bucket = client_GCS.get_bucket(bucket_name)
        client_bucket.blob(file_name).upload_from_string(
            file_str)

    # Initialize variables
    bucket_name = event['bucket']
    blob_name = event['name']
    directory_path = "gs://" + bucket_name + "/"
    source_file_path = directory_path + blob_name
    schema_file_path = directory_path + os.environ.get('SCHEMA_FILE_NAME')

    var_year = str(datetime.datetime.utcnow().strftime('%Y'))
    var_datetime = str(datetime.datetime.utcnow().strftime('%d%m%y'))
    destination_file_path = "./TRANSFORMED/centanent/{0}/TRANSFORMED_centanet_2022_centanet_property_transaction_{1}.csv".format(
        var_year, var_datetime)

    # Run query to get last inserted date from table

    latest_inserted_date = str(query_data_from_table("project-diver","project-diver","dbo.property_transaction","SELECT Inserted_Date_Api FROM dbo.property_transaction ORDER BY Inserted_Date_Api DESC LIMIT 1")["Inserted_Date_Api"][0])
       
    ## Read data
    df_raw_property_transaction = pd.read_parquet(source_file_path)

    ## read schema configuration
    df_schema = pd.read_csv(schema_file_path)[
        ["Source Column", "Target Column"]].dropna(axis=0)

    ## putting column names into a list - used for extracting 
    list_of_source_columns = df_schema["Source Column"].to_list()
    dict_column_mapping = dict(
        zip(df_schema["Source Column"], df_schema["Target Column"]))

    ## cleaning data
    try:
        df_intermediate = pd.concat([df_raw_property_transaction.drop(['picture', 'scope'], axis=1), df_raw_property_transaction["picture"].apply(
        pd.Series), df_raw_property_transaction["scope"].apply(pd.Series)], axis=1)[list_of_source_columns]
    except Exception as e:
        output_json['Error'] = True
        output_json['Failed Stage'] = 'expand dictionary in columns'
        output_json['Error Message'] = str(e)
    try:
        df_intermediate['insDate'] = pd.to_datetime(df_intermediate['insDate'])
    except Exception as e:
        output_json['Error'] = True
        output_json['Failed Stage'] = 'Query table'
        output_json['Error Message'] = str(e)
    
    df_intermediate = df_intermediate[(df_intermediate['insDate']>datetime.datetime.strptime(latest_inserted_date,"%Y-%m-%d %H:%M:%S"))]
    df_intermediate = df_intermediate.explode("floorPlans")
    df_intermediate = df_intermediate.replace({'postType': {'R': 'Rent',
                                                            'S': 'Sale'},
                                                'unitType': {'AP': 'Apartment',
                                                            'HS': 'House'}})

    df_clean = df_intermediate.rename(columns=dict_column_mapping)
    df_clean["Source_Flag"] = "CENTANET"

    try:
        load_count = load_data_to_table("project-diver","dbo.property_transaction", df_clean).output_rows
        output_json['Load to table'] = 'Succeeded'
        output_json['Rows inserted count'] = load_count
    except Exception as e:
        output_json['Error'] = True
        output_json['Failed Stage'] = 'Load'
        output_json['Error Message'] = str(e)

    try:
        upload_blob("project-diver-dev", destination_file_path, df_clean.to_parquet())
        output_json['Loop'] = 'Succeeded'
    except Exception as e:
        output_json['Error'] = True
        output_json['Failed Stage'] = 'Load'
        output_json['Error Message'] = str(e)

    print(output_json)
    return jsonify(output_json)
