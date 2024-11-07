from influxdb import InfluxDBClient

def store_to_influxdb(data, measurement='gold_layer'):
    # Connect to InfluxDB (configured with Docker)
    client = InfluxDBClient(host='influxdb', port=8086)
    client.switch_database('mydatabase')  # Ensure database exists

    # Format data for InfluxDB
    influx_data = [
        {
            "measurement": measurement,
            "tags": {
                "source": data['SourceID'],
                "city": data['CurrentArea']
            },
            "time": data['TrxnTimestamp'],
            "fields": {
                "amount": data['Amount'],
                "km": data['KM'],
                "car_status": data['CarDrivingStatus']
                # Add other fields as required
            }
        }
    ]
    
    # Write data to InfluxDB
    client.write_points(influx_data)
    client.close()
