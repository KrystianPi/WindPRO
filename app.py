from flask import Flask, request, jsonify
from main import predict, monitor, retrain

app = Flask(__name__)

@app.route('/predict', methods=['POST'])
def api_predict():
    station = request.json.get('station', 'rewa')
    RUN_ID = request.json.get('RUN_ID', '1b96b8edbddb4e95866a5431b25becd0')
    predict(station, RUN_ID)
    return jsonify({"message": "Prediction completed!"})

@app.route('/monitor', methods=['POST'])
def api_monitor():
    station = request.json.get('station', 'rewa')
    RUN_ID = request.json.get('RUN_ID', '1b96b8edbddb4e95866a5431b25becd0')
    monitor(station, RUN_ID)
    return jsonify({"message": "Monitor completed!"})

@app.route('/retrain', methods=['POST'])
def api_retrain():
    station = request.json.get('station', 'rewa')
    retrain(station)
    return jsonify({"message": "Retraining completed!"})

if __name__ == '__main__':
    app.run(debug=True, port=5000)