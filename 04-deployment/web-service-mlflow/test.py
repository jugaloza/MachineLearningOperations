import requests

data = {
    'PULocationID' : 10,
    'DOLocationID' : 20,
    'trip_distance' : 40
}


url = "http://localhost:5000/get_data"
requests.post(url,json=data)