# import pandas as pd
# from sklearn.feature_extraction import DictVectorizer
# from sklearn.linear_model import LinearRegression,Lasso
# from sklearn.metrics import mean_squared_error
# import seaborn as sns
# import matplotlib.pyplot as plt
# import pickle
# import mlflow
# import xgboost as xgb

# from hyperopt import fmin,tpe,hp,STATUS_OK,Trials
# from hyperopt.pyll import scope

# from prefect import flow,task
# from prefect.task_runners import SequentialTaskRunner

# @task(retries=3)
# def read_dataframe(file_name):
    
#     if file_name.endswith('.csv'):
#         data = pd.read_csv('data/'+file_name)
        
#     else:
#         data = pd.read_parquet(file_name)
        
    
#     data['duration'] = data.lpep_dropoff_datetime - data.lpep_pickup_datetime
        
#     data.duration = data.duration.apply(lambda dt: dt.total_seconds()/60)
        
#     categorical = ['PULocationID','DOLocationID']
#     numerical = ['trip_distance']
    
#     data = data[(data.duration >= 1) & (data.duration <= 60)]
    
#     data[categorical] = data[categorical].astype(str)

#     return data

# @task(retries=3)        
# def add_features(train_data,val_data):
#     #print(train_path)
#     #train_data = read_dataframe(train_path)
#     #val_data = read_dataframe(val_path)

#     print(train_data.shape,val_data.shape)
    
#     train_data['PUDO'] = train_data['PULocationID'] + '_' + train_data['DOLocationID']
#     val_data['PUDO'] = val_data['PULocationID'] + '_' + val_data['DOLocationID']
#     categorical = ['PUDO']
#     numerical = ['trip_distance']

#     dv = DictVectorizer()


#     train_dicts = train_data[categorical+numerical].to_dict(orient='Records')
#     x_train = dv.fit_transform(train_dicts)
#     y_train = train_data.duration.values

#     val_dicts = val_data[categorical+numerical].to_dict(orient='records')
#     x_val = dv.transform(val_dicts)
#     y_val = val_data.duration.values

#     print(x_train.shape)
#     return x_train,x_val,y_train,y_val,dv


# @task(retries=3)
# def train_model_search(train,valid,y_val):
#     def objective(params):
#         with mlflow.start_run():
#             mlflow.set_tag("model","xgboost")
#             mlflow.log_params(params)
        
#             booster = xgb.train(params=params,dtrain=train,num_boost_round=50,evals=[(valid,"validation")],early_stopping_rounds=50)
        
#             y_pred = booster.predict(valid)
        
#             rmse = mean_squared_error(y_val,y_pred,squared=False)
        
#             mlflow.log_metric("RMSE",rmse)
        
#         return {"loss":rmse,"status": STATUS_OK}


#     search_space = {
#     'max_depth' : scope.int(hp.quniform('max_depth',4,100,1)),
#     'learning_rate' : hp.loguniform('learning_rate',-3,0),
#     'reg_alpha' : hp.loguniform('reg_alpha',-5,-1),
#     'reg_lambda' : hp.loguniform('reg_lambda',-6,-1),
#     'min_child_weight' : hp.loguniform('min_child_weight',-1,3),
#     'objective' : 'reg:linear',
#     'seed' : 42
#     }

#     best_result = fmin(fn=objective,
#                   space=search_space,
#                   algo = tpe.suggest,
#                   max_evals=1,
#                   trials=Trials())

#     return

# @task(retries=3)
# def train_best_model(train,valid,y_val,dv):

#     params = {
#     'learning_rate':0.38224953779852067,
#     'max_depth':94,
#     'min_child_weight':0.8380472896224769,
#     'objective': 'reg:linear',
#     'reg_alpha':0.08594677002588899,
#     'reg_lambda':0.17515937955354294,

#     }

#     with mlflow.start_run():
#         #train = xgb.DMatrix(x_train,label=y_train)
#         #valid = xgb.DMatrix(x_val,label=y_val)
    
#         mlflow.log_params(params)
    
#         with open('preprocessor.b','wb') as f:
#             pickle.dump(dv,f)
        
#         mlflow.log_artifact('preprocessor.b','Preprocessors')
    
#         booster = xgb.train(params,dtrain=train,num_boost_round=1000,evals=[(valid,'validation')],early_stopping_rounds=50)
    
#         y_pred = booster.predict(valid)
    
#         rmse = mean_squared_error(y_val,y_pred,squared=False)
    
#         mlflow.log_metric("RMSE",rmse)
    
#         mlflow.xgboost.log_model(booster,artifact_path="models_mlflow")




# # if __name__ == '__main__':
# @flow(task_runner=SequentialTaskRunner())
# def main(train_path=r"D:\mlops_zoomcamp_clone\data\green_tripdata_2021-01.parquet",val_path=r"D:\mlops_zoomcamp_clone\data\green_tripdata_2021-02.parquet"):
    
#     mlflow.set_tracking_uri("sqlite:///mlflow.db")
#     mlflow.set_experiment("first-mlflow-experiement")   
#     X_train = read_dataframe(train_path)
#     X_val = read_dataframe(val_path)
    
#     x_train,x_val,y_train,y_val,dv = add_features(X_train,X_val).result()
#     train = xgb.DMatrix(x_train,label=y_train)
#     valid = xgb.DMatrix(x_val,label=y_val)

#     train_model_search(train,valid,y_val)
#     train_best_model(train,valid,y_val,dv)

# main()

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression, Lasso, Ridge
from sklearn.metrics import mean_squared_error

import xgboost as xgb

from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll import scope
import pickle
import mlflow

from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

@task
def read_dataframe(filename):
    df = pd.read_parquet(filename)

    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    
    return df

@task
def add_features(df_train, df_val):
    # df_train = read_dataframe(train_path)
    # df_val = read_dataframe(val_path)

    print(len(df_train))
    print(len(df_val))

    df_train['PU_DO'] = df_train['PULocationID'] + '_' + df_train['DOLocationID']
    df_val['PU_DO'] = df_val['PULocationID'] + '_' + df_val['DOLocationID']

    categorical = ['PU_DO'] #'PULocationID', 'DOLocationID']
    numerical = ['trip_distance']

    dv = DictVectorizer()

    train_dicts = df_train[categorical + numerical].to_dict(orient='records')
    X_train = dv.fit_transform(train_dicts)

    val_dicts = df_val[categorical + numerical].to_dict(orient='records')
    X_val = dv.transform(val_dicts)

    target = 'duration'
    y_train = df_train[target].values
    y_val = df_val[target].values

    return X_train, X_val, y_train, y_val, dv

@task
def train_model_search(train, valid, y_val):
    def objective(params):
        with mlflow.start_run():
            mlflow.set_tag("model", "xgboost")
            mlflow.log_params(params)
            booster = xgb.train(
                params=params,
                dtrain=train,
                num_boost_round=100,
                evals=[(valid, 'validation')],
                early_stopping_rounds=50
            )
            y_pred = booster.predict(valid)
            rmse = mean_squared_error(y_val, y_pred, squared=False)
            mlflow.log_metric("rmse", rmse)

        return {'loss': rmse, 'status': STATUS_OK}

    search_space = {
        'max_depth': scope.int(hp.quniform('max_depth', 4, 100, 1)),
        'learning_rate': hp.loguniform('learning_rate', -3, 0),
        'reg_alpha': hp.loguniform('reg_alpha', -5, -1),
        'reg_lambda': hp.loguniform('reg_lambda', -6, -1),
        'min_child_weight': hp.loguniform('min_child_weight', -1, 3),
        'objective': 'reg:linear',
        'seed': 42
    }

    best_result = fmin(
        fn=objective,
        space=search_space,
        algo=tpe.suggest,
        max_evals=1,
        trials=Trials()
    )
    return

@task
def train_best_model(train, valid, y_val, dv):
    with mlflow.start_run():

        best_params = {
            'learning_rate': 0.09585355369315604,
            'max_depth': 30,
            'min_child_weight': 1.060597050922164,
            'objective': 'reg:linear',
            'reg_alpha': 0.018060244040060163,
            'reg_lambda': 0.011658731377413597,
            'seed': 42
        }

        mlflow.log_params(best_params)

        booster = xgb.train(
            params=best_params,
            dtrain=train,
            num_boost_round=100,
            evals=[(valid, 'validation')],
            early_stopping_rounds=50
        )

        y_pred = booster.predict(valid)
        rmse = mean_squared_error(y_val, y_pred, squared=False)
        mlflow.log_metric("rmse", rmse)

        with open("models/preprocessor.b", "wb") as f_out:
            pickle.dump(dv, f_out)
        mlflow.log_artifact("models/preprocessor.b", artifact_path="preprocessor")

        mlflow.xgboost.log_model(booster, artifact_path="models_mlflow")

@flow(task_runner=SequentialTaskRunner())
def main(train_path: str=r"D:\mlops_zoomcamp_clone\data\green_tripdata_2021-01.parquet",
        val_path: str=r"D:\mlops_zoomcamp_clone\data\green_tripdata_2021-02.parquet"):
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("nyc-taxi-experiment")
    X_train = read_dataframe(train_path)
    X_val = read_dataframe(val_path)
    X_train, X_val, y_train, y_val, dv = add_features(X_train, X_val).result()
    train = xgb.DMatrix(X_train, label=y_train)
    valid = xgb.DMatrix(X_val, label=y_val)
    train_model_search(train, valid, y_val)
    train_best_model(train, valid, y_val, dv)