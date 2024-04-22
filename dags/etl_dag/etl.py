import pandas as pd
import logging
import json
import sys
import os

sys.path.append(os.path.abspath("/opt/airflow/"))

from transformations.spotify_transformations import *
from transformations.grammys_transformations import *
from transformations.after_merge import *
from dags.etl_dag.db import *




from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive



credentials_dir = 'credentials_module.json'


def extract_csv():

    logging.info("starting extraction process")

    df = pd.read_csv('./plugins/spotify_dataset.csv')

    logging.info("extraction finished")
    logging.info('data extracted is %s', df.head(5))

    return df.to_json(orient='records')


def transform_csv(**kwargs):

    logging.info("starting transformation process")
    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids='extraction_csv')
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info('data to transform is %s', df.head(5))

    df = drop_unnamed_0_column(df)

    df = drop_duplicates(df)
    
    df = drop_missing_values(df)

    df = create_popularity_level_column(df)

    df = create_duration_mins_secs_column(df)

    df = create_is_explicit_column(df)
    
    df = create_is_danceable_column(df)

    df = create_track_feeling_column(df)

    df = create_general_category_column(df)
    
    df = drop_columns(df)

    logging.info("transformation finished %s", df.head(5))
    return df.to_json(orient='records')


def extract_db():
   
    query = "SELECT * FROM grammys"
    
    engine = engine_creation()
    
    session = create_session(engine)
    
    create_table(engine)
    
    insert_data()
    
    df_grammys = pd.read_sql(query, con=engine)

    #Cerramos la conexion a la db
    finish_engine(engine)

    logging.info("database read succesfully")
    logging.info('data extracted is %s', df_grammys.head(5))
    return df_grammys.to_json(orient='records')


def transform_db(**kwargs):
    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids='extraction_db')
    json_data = json.loads(str_data)
    df_grammys = pd.json_normalize(data=json_data)
    logging.info("starting transformation process")

    df_grammys = replacing_nulls_cond_1(df_grammys)
    df_grammys = replacing_nulls_cond_2(df_grammys)
    df_grammys = replacing_nulls_cond_3(df_grammys)
    df_grammys = replacing_nulls_cond_4(df_grammys)
    df_grammys = replacing_nulls_cond_5(df_grammys)
    df_grammys = replacing_nulls_cond_6(df_grammys)
    df_grammys = replacing_nulls_cond_7(df_grammys)

    df_grammys = removing_nulls(df_grammys)
    logging.info('removing nulls finished %s', df_grammys.isnull().sum())
    df_grammys = removing_unnecessary_columns(df_grammys)
    logging.info('removing unnecessary columns finished %s', df_grammys.columns)
    df_grammys = rename_columns(df_grammys)
    logging.info('renaming columns finished %s', df_grammys.columns)
    df_grammys = was_nominated_mapping(df_grammys)
    logging.info('mapping was nominated finished %s', df_grammys['was_nominated'].unique())

    logging.info('data transformed is %s', df_grammys.head(5))
    return df_grammys.to_json(orient='records')

def merge(**kwargs):
    ti = kwargs['ti']

    logging.info("starting merge process")
    str_data = ti.xcom_pull(task_ids='transformation_csv')
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    str_data = ti.xcom_pull(task_ids='transformation_db')
    json_data = json.loads(str_data)
    df_grammys = pd.json_normalize(data=json_data)

    df_merged = df.merge(df_grammys, how='left', left_on='track_name', right_on='nominee')
    logging.info('data merged is %s', df_merged.head(5))
    logging.info('data merged shape is %s', df_merged.shape)
    logging.info('data merged columns are %s', df_merged.columns)
    logging.info("merge finished")

    logging.info("Adjusting the merge")
    df_merged = fill_null_values_was_nomineed(df_merged)
    df_merged = fill_null_values_year(df_merged)
    df_merged = fill_null_values_nominee(df_merged)
    df_merged = fill_null_values_remaining(df_merged)
    df_merged = drop_artist_column(df_merged)
    logging.info('data merged adjusted is %s', df_merged.head(5))

    return df_merged.to_json(orient='records')

def load(**kwargs):
    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids='merge')
    json_data = json.loads(str_data)
    df_merged = pd.json_normalize(data=json_data)

    logging.info("starting load process")
    engine = engine_creation()
    create_merge_table(engine)
    insert_merge(df_merged)
    finish_engine(engine)
    logging.info("data loaded in: MusicAwards")

    return df_merged.to_json(orient='records')



def login_drive():
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_dir)

    if gauth.credentials is None:
        gauth.Refresh()
        gauth.SaveCredentialsFile(credentials_dir)
    else:
        gauth.Authorize()

    return GoogleDrive(gauth)

def store(**kwargs):
    logging.info("starting store process")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="load")
    json_data = json.loads(str_data)
    df_merged = pd.json_normalize(data=json_data)
    
    logging.info("Data stored successfully")

    drive = login_drive()

    csv_merge = df_merged.to_csv(index=False)

    file = drive.CreateFile({'title': 'merged.csv',
                             'parents': [{'kind': 'drive#fileLink', 'id': '1YEHq7Aj9FkEoX2cMo5r3s8XtmgWFBSfR'}],
                             'mimeType': 'text/csv'})
    
    file.SetContentString(csv_merge)
    file.Upload()

    




    


# def load(json_data):
#     logging.info("starting load process")
#     #data = pd.json_normalize(data=json_data)
#     logging.info( f"data to load is: {json_data}")
#     logging.info("Loading data")
#     #TODO: do the load here
#     logging.info( "data loaded in: table_name")