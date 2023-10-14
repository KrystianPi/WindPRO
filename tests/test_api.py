import requests


def test_predict_endpoint():
    url = "http://127.0.0.1:8000/predict"
    params = {
        "station": "rewa",
        "model_name": "xgboost-8features-hpt",
        "version": 2
    }

    response = requests.post(url, params=params)
    assert response.status_code == 200, f"Expected status code 200, but got {response.status_code}"

    data = response.json()
    # Assert that the 'predictions' key exists and contains a list of floats
    assert 'predictions' in data, "The 'predictions' key was not found in the response."
    assert isinstance(data['predictions'], list), "The 'predictions' key does not contain a list."
    assert all(isinstance(item, float) for item in data['predictions']), "Not all items in 'predictions' are floats."