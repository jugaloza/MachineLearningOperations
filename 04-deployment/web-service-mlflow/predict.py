
from flask import Flask,request,jsonify

import mlflow

RUN_ID = 'fb055dbbf64c49dbbf00483718920aee'
logged_model = f'runs:/{RUN_ID}/models'


model = mlflow.pyfunc.load_model(logged_model)


app = Flask("duration-prediction")

@app.route('/')
def home():
    return '<h1> Home page </h1>'

def prepare_dataset(data):
    
    features = {}
    features['PU_DO'] = '%s_%s' % (data['PULocationID'],data['DOLocationID'])
    features['trip_distance'] = data['trip_distance']

    return features

def predict(features):
    results = model.predict(features)
    return float(results[0])

@app.route('/predict',methods=['POST'])
def predict():
    data = request.get_json()

    features = prepare_dataset(data)

    preds = predict(features)

    results = {
        'duration' : preds,
        'model_version' : RUN_ID

    }


    return jsonify(results)

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9696)