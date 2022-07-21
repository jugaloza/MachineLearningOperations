import json
import pickle

from flask import Flask,request,jsonify

with open('lin_reg.bin','rb') as f_in:
    dv,model = pickle.load(f_in)


def prepare_features(ride):

    features = {}

    features['PUDO'] = '%s_%s' % (ride['PULocationID'],ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']

    return features

def predict(data):

    data = dv.transform(data)

    predictions = model.predict(data)

    return predictions

app = Flask('duration-prediction')

@app.route('/predict',methods=['POST'])
def predict_endpoint():
    ride = request.get_json()

    features = prepare_features(ride)

    predictions = predict(features)

    result = {
        'predictions' : predictions[0]
    }
    
    return jsonify(result)

if __name__ == '__main__':

    app.run(debug=True,host='0.0.0.0',port=9696)
#print(dv,model)