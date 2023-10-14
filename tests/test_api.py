import requests


def test_predict_endpoint():
    url = "http://127.0.0.1:8000/predict"
    params = {
        "station": "rewa",
        "experiment_name":'xgb_hpt_cv_x1_testing',
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

def test_predict_endpoint():
    url = "http://127.0.0.1:8000/monitor"
    params = {
        "station": "rewa",
        "experiment_name":'xgb_hpt_cv_x1_testing',
        "model_name": "xgboost-8features-hpt",
        "version": 2
    }

    response = requests.post(url, params=params)
    assert response.status_code == 200, f"Expected status code 200, but got {response.status_code}"

    data = response.json()
    # Assert that the 'predictions' key exists and contains a list of floats
    assert 'r2 score' in data, "The 'r2 score' key was not found in the response."
    assert isinstance(data['r2 score'], float), "The 'r2 score' is not float"
    assert -float('inf') <= data['r2 score'] <= 1.0, "The 'r2 score' is not in the range [-inf, 1.0]"