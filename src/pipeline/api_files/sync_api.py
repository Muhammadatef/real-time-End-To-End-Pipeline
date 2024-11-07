from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/sync', methods=['POST'])
def sync():
    # Get the JSON payload from the request
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # Here you would process the data as needed
    # For now, we're just logging it to simulate processing
    batch_id = data.get("BatchID", "N/A")
    print(f"Received record with BatchID: {batch_id}")

    # Simulate a successful response
    return jsonify({"status": "success", "BatchID": batch_id}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
