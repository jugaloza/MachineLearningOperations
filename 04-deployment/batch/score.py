import pandas as pd
import pickle
import mlflow
import sys
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.feature_extraction import DictVectorizer
import uuid
import os
from datetime import datetime
from prefect import task,flow
from prefect.task_runners import SequentialTaskRunner
from dateutil.relativedelta import relativedelta
from prefect.context import get_run_context

 
def generate_uuids(n):
    #n = len(df)
    ride_ids = []

    for i in range(n):
        ride_ids.append(str(uuid.uuid4()))  

    return ride_ids



def read_dataframe(path):

    print('Reading Data from dataframe')
    df = pd.read_parquet(path)
    
    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    
    df.duration = df.duration.dt.total_seconds() / 60
    
    df = df[(df.duration > 1) & (df.duration < 60)]
    
    categorical = ['PULocationID','DOLocationID']
    
    df[categorical] = df[categorical].astype(str)
    
    return df

def prepare_dictionaries(df: pd.DataFrame):

    print('Preparing Data')
    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    
    df['PU_DO'] = df['PULocationID'] + '_' + df['DOLocationID']

    categorical = ['PU_DO']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')
    
    return dicts

def load_model(run_id):
    logged_model = f'runs:/{run_id}/models'
    model = mlflow.pyfunc.load_model(logged_model)
    return model
def apply_model(input_file,run_id,output_file):
    df = read_dataframe(input_file)

    print('Data Reading completed successfully')
    dicts = prepare_dictionaries(df)

    print('Data Preparation completed')
    model = load_model(run_id)
    with open('DictVectorizer.bin','rb') as f:
        dv = pickle.load(f)
        x = dv.transform(dicts)
        #logged_model = 'runs:/8cf38dfad1a24ceb9b58de03cc679dd9/models'

    print('Model and Dictonary Vectorizer loaded successfully')

    print('Inferencing on dataset')
    y_pred = model.predict(x)

    df_result = pd.DataFrame()

    df_result['ride_id'] = generate_uuids(len(df))
    df_result['lpep_pickup_datetime'] = df['lpep_pickup_datetime']
    df_result['PULocationID'] = df['PULocationID']
    df_result['DOLocationID'] = df['DOLocationID']
    df_result['actual_duration'] = df['duration']
    df_result['predicted_duration'] = y_pred
    df_result['diff'] = df_result['actual_duration'] - df_result['predicted_duration']
    df_result['model_version'] = run_id


    df_result.to_parquet(output_file,index=False)

    print(f'Results storead at : {output_file}')

@flow(task_runner=SequentialTaskRunner())
def ride_duration_prediction(taxi_type: str,run_id: str,run_date: datetime = None):
    if run_date is None:
        ctx = get_run_context()
        run_date = ctx.flow_run.expected_start_time
    
    prev_month = run_date - relativedelta(months=1)
    year = prev_month.year
    month = prev_month.month

    input_file = f'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'output/{taxi_type}/tripdata_{year:04d}-{month:02d}.parquet'
    apply_model(input_file,
                run_id,
                output_file)




def run():
    taxi_type = sys.argv[1]
    year = int(sys.argv[2])
    month = int(sys.argv[3])
    RUN_ID = sys.argv[4] # 8cf38dfad1a24ceb9b58de03cc679dd9
    
    
    mlflow.set_tracking_uri('http://127.0.0.1:5000')
    mlflow.set_experiment('random-forest-experiment')
    
    ride_duration_prediction(taxi_type,run_id=RUN_ID,run_date=datetime(year=year,month=month,day=1))


if __name__ == '__main__':
    run()


