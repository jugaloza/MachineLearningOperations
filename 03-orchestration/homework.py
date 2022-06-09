import pandas as pd

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from datetime import timedelta
from prefect import flow,get_run_logger,task
import datetime
import mlflow
import pickle
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule,IntervalSchedule
from prefect.flow_runners import SubprocessFlowRunner


@task
def read_data(path):
    df = pd.read_parquet(path)
    return df

@task
def prepare_features(df, categorical, train=True):
    logger = get_run_logger()
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    mean_duration = df.duration.mean()
    if train:
        logger.info(f"The mean duration of training is {mean_duration}")
        #print(f"The mean duration of training is {mean_duration}")
        #mlflow.log('Train duration',mean_duration)
    else:
        logger.info(f"The mean duration of validation is {mean_duration}")
        #print(f"The mean duration of validation is {mean_duration}")
    
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df

@task
def train_model(df, categorical):

    logger = get_run_logger()
    train_dicts = df[categorical].to_dict(orient='records')
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts) 
    y_train = df.duration.values

    logger.info(f"The shape of X_train is {X_train.shape}")
    logger.info(f"The DictVectorizer is {len(dv.feature_names_)}")
    #print(f"The shape of X_train is {X_train.shape}")
    #print(f"The DictVectorizer has {len(dv.feature_names_)} features")

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    mse = mean_squared_error(y_train, y_pred, squared=False)
    #print(lr,dv)
    #mlflow.log
    logger.info(f"The MSE of training is {mse}")
    #print(f"The MSE of training is: {mse}")
    return lr, dv


@task
def run_model(df, categorical, dv, lr):

    logger = get_run_logger()

    val_dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(val_dicts) 
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    mse = mean_squared_error(y_val, y_pred, squared=False)

    logger.info(f"The MSE of validation is {mse}")

    mlflow.log_metric('MSE',mse)
    #mlflow.log_artifact()
    #print(f"The MSE of validation is: {mse}")
    return

@task
def get_paths(date):


    if date is None:

        date = datetime.today()

        train_month = date.month() - 2
        val_month = date.month() - 1

        year = date.year()

        train_file_name = 'fhv_tripdata_'+str(year)+'-'+str(train_month) + '.parquet'
        #train_data = pd.read_parquet(train_file_name)
        val_file_name = 'fhv_tripdata_'+str(year)+'-'+str(val_month)+ '.parquet'
        # val_data = pd.read_parquet(val_file_name)
    else:
        year = int(date.split('-')[0])
        month = int(date.split('-')[1])
        day = int(date.split('-')[2])

        date = datetime.date(year=year,month=month,day=day)

        train_month = date.month - 2
        val_month = date.month - 1

        year = date.year

        train_file_name = 'fhv_tripdata_'+str(year)+'-0'+str(train_month)+'.parquet'
        val_file_name = 'fhv_tripdata_'+str(year)+'-0'+str(val_month)+'.parquet'

        # train_data = pd.read_parquet(train_file_name)
        # val_data = pd.read_parquet(val_file_name)
    return train_file_name,val_file_name
        

@flow
def main(date=None):

    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("FHV_Trip_Experiment-1")

    with mlflow.start_run():
        categorical = ['PUlocationID','DOlocationID']

        train_path,val_path = get_paths(date).result()

        df_train = read_data('data/'+train_path)
        df_train_processed = prepare_features(df_train,categorical)

        df_val = read_data('data/'+val_path)
        df_val_processed = prepare_features(df_val,categorical)

        lr,dv = train_model(df_train_processed,categorical).result()

        run_model(df_val_processed,categorical,dv,lr)

        model_file_name = f"model-{date}.bin"
        with open(model_file_name,'wb') as f:
            pickle.dump(lr,f)

        dv_file_name = f"dv-{date}.bin"
        with open(dv_file_name,'wb') as f:
            pickle.dump(dv,f)








# def main(train_path: str = './data/fhv_tripdata_2021-01.parquet', 
#            val_path: str = './data/fhv_tripdata_2021-02.parquet'):

#     categorical = ['PUlocationID', 'DOlocationID']

#     df_train = read_data(train_path)
#     df_train_processed = prepare_features(df_train, categorical)

#     df_val = read_data(val_path)
#     df_val_processed = prepare_features(df_val, categorical)

#     # train the model
#     lr, dv = train_model(df_train_processed, categorical)
#     run_model(df_val_processed, categorical, dv, lr)


#main(date="2021-08-15")

#schedule = CronSchedule(cron='0 9 15 * *',timezone="America/New_York")

DeploymentSpec(
    flow=main,
    name="model-training",
    schedule=CronSchedule(cron='0 9 15 * *',timezone="America/New_York"),
    flow_runner=SubprocessFlowRunner(),
    tags=["ml"]
)

