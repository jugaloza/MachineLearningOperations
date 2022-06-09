import pandas as pd
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression,Lasso
from sklearn.metrics import mean_squared_error
import seaborn as sns
import matplotlib.pyplot as plt
import pickle
import mlflow
import xgboost as xgb

from hyperopt import fmin,tpe,hp,STATUS_OK,Trials
from hyperopt.pyll import scope


mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("first-mlflow-experiement")

def read_dataframe(file_name):
    
    if file_name.endswith('.csv'):
        data = pd.read_csv('data/'+file_name)
        
    else:
        data = pd.read_parquet(file_name)
        
    
    data['duration'] = data.lpep_dropoff_datetime - data.lpep_pickup_datetime
        
    data.duration = data.duration.apply(lambda dt: dt.total_seconds()/60)
        
    categorical = ['PULocationID','DOLocationID']
    numerical = ['trip_distance']
    
    data = data[(data.duration >= 1) & (data.duration <= 60)]
    
    data[categorical] = data[categorical].astype(str)

    return data

        
def add_features(train_path = 'green_tripdata_2021-01.parquet',val_path ='green_tripdata_2021-02.parquet'):
    #print(train_path)
    train_data = read_dataframe(train_path)
    val_data = read_dataframe(val_path)

    #print(train_data.shape,val_data.shape)
    
    train_data['PUDO'] = train_data['PULocationID'] + '_' + train_data['DOLocationID']
    val_data['PUDO'] = val_data['PULocationID'] + '_' + val_data['DOLocationID']
    categorical = ['PUDO']
    numerical = ['trip_distance']

    dv = DictVectorizer()


    train_dicts = train_data[categorical+numerical].to_dict(orient='Records')
    x_train = dv.fit_transform(train_dicts)
    y_train = train_data.duration.values

    val_dicts = val_data[categorical+numerical].to_dict(orient='records')
    x_val = dv.transform(val_dicts)
    y_val = val_data.duration.values

    return x_train,x_val,y_train,y_val,dv



def train_model_search(train,valid,y_val):
    def objective(params):
        with mlflow.start_run():
            mlflow.set_tag("model","xgboost")
            mlflow.log_params(params)
        
            booster = xgb.train(params=params,dtrain=train,num_boost_round=50,evals=[(valid,"validation")],early_stopping_rounds=50)
        
            y_pred = booster.predict(valid)
        
            rmse = mean_squared_error(y_val,y_pred,squared=False)
        
            mlflow.log_metric("RMSE",rmse)
        
        return {"loss":rmse,"status": STATUS_OK}


    search_space = {
    'max_depth' : scope.int(hp.quniform('max_depth',4,100,1)),
    'learning_rate' : hp.loguniform('learning_rate',-3,0),
    'reg_alpha' : hp.loguniform('reg_alpha',-5,-1),
    'reg_lambda' : hp.loguniform('reg_lambda',-6,-1),
    'min_child_weight' : hp.loguniform('min_child_weight',-1,3),
    'objective' : 'reg:linear',
    'seed' : 42
    }

    best_result = fmin(fn=objective,
                  space=search_space,
                  algo = tpe.suggest,
                  max_evals=1,
                  trials=Trials())

    return


def train_best_model(train,valid,y_val):

    params = {
    'learning_rate':0.38224953779852067,
    'max_depth':94,
    'min_child_weight':0.8380472896224769,
    'objective': 'reg:linear',
    'reg_alpha':0.08594677002588899,
    'reg_lambda':0.17515937955354294,

    }

    with mlflow.start_run():
        #train = xgb.DMatrix(x_train,label=y_train)
        #valid = xgb.DMatrix(x_val,label=y_val)
    
        mlflow.log_params(params)
    
        with open('preprocessor.b','wb') as f:
            pickle.dump(dv,f)
        
        mlflow.log_artifact('preprocessor.b','Preprocessors')
    
        booster = xgb.train(params,dtrain=train,num_boost_round=1000,evals=[(valid,'validation')],early_stopping_rounds=50)
    
        y_pred = booster.predict(valid)
    
        rmse = mean_squared_error(y_val,y_pred,squared=False)
    
        mlflow.log_metric("RMSE",rmse)
    
        mlflow.xgboost.log_model(booster,artifact_path="models_mlflow")




if __name__ == '__main__':
    x_train,x_val,y_train,y_val,dv = add_features(train_path=r"D:\mlops_zoomcamp_clone\data\green_tripdata_2021-01.parquet",val_path=r"D:\mlops_zoomcamp_clone\data\green_tripdata_2021-02.parquet")
    train = xgb.DMatrix(x_train,label=y_train)
    valid = xgb.DMatrix(x_val,label=y_val)

    train_model_search(train,valid,y_val)
    train_best_model(train,valid,y_val)
#########
#modelling section


# lr = LinearRegression()
# lr.fit(x_train,y_train)

# y_pred = lr.predict(x_val)

# mean_squared_error(y_val,y_pred,squared=False)

# with open('models/lin_reg.bin','wb') as f:
#     pickle.dump((dv,lr),f)


# with mlflow.start_run():
    
#     mlflow.set_tag("model","Lasso Regression")
#     mlflow.log_param("train-data-path",".\data\green_tripdata_2021-01.parquet")
#     mlflow.log_param("val-data-path",".\data\green_tripdata_2021-02.parquet")
    
#     alpha = 0.1
    
#     mlflow.log_param("Alpha",alpha)
    
#     lasso = Lasso(alpha)
#     lasso.fit(x_train,y_train)

#     y_pred = lasso.predict(x_val)

#     metrics = mean_squared_error(y_val,y_pred,squared=False)
    
#     mlflow.log_metric("RMSE",metrics)
    





# # mlflow.xgboost.autolog()

# # booster = xgb.train(params=params,dtrain=train,num_boost_round=1000,evals=[(valid,"validation")],early_stopping_rounds=50)

