import predict
import requests
ride = {
    'PULocationID': 50,
    'DOLocationID': 50,
    'trip_distance': 40
}

url = 'http://localhost:9696/predict'

results = requests.post(url=url,json=ride)

print(results.json())

# features = predict.prepare_features(ride)
# pred = predict.predict(features)

# print(pred[0])