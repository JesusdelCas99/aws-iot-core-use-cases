import paho.mqtt.client as mqtt
import ssl
import json
import time
import random
from datetime import datetime


# Function to create random sensor data
def generate_telemetry_data():
    return {
        "type": "telemetry",
        "humidity": random.randint(40, 60),
        "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    }

# MQTT connection callback function
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print(f"Connection failed: {rc}")

# MQTT message publish callback function
def on_publish(client, userdata, mid):
    print(f"Message {mid} published")
    

if __name__ == "__main__": 

	# Paths to certificates and private key
	certificate_path = './connect_device_package/iot-sensor.cert.pem'
	private_key_path = './connect_device_package/iot-sensor.private.key'
	ca_cert_path = './connect_device_package/root-CA.crt'
	
	# Set up MQTT client with TLS
	mqtt_client = mqtt.Client()
	mqtt_client.tls_set(
	    ca_certs=ca_cert_path,
	    certfile=certificate_path,
	    keyfile=private_key_path,
	    tls_version=ssl.PROTOCOL_TLSv1_2
	)
	
	# AWS IoT Core endpoint and port
	iot_endpoint = 'a1b2wejtjov70w-ats.iot.eu-west-1.amazonaws.com'
	port = 8883
	
	# MQTT topic to publish data
	topic = 'sensor_data'
	
	# Assign callback functions
	mqtt_client.on_connect = on_connect
	mqtt_client.on_publish = on_publish
	
	# Connect to AWS IoT Core
	mqtt_client.connect(iot_endpoint, port, keepalive=60)
	
	# Start MQTT loop
	mqtt_client.loop_start()
	
	# Main loop to send data every 30 seconds
	while True:
	    telemetry_data = generate_telemetry_data()  # Generate data
	    json_data = json.dumps(telemetry_data)  # Convert to JSON
	
	    try:
	        mqtt_client.publish(topic, json_data, qos=1)  # Send data
	        print("Telemetry data sent to AWS IoT Core.")
	    except Exception as e:
	        print(f"Error sending data: {e}")
	
	    time.sleep(30)  # Wait for 30 seconds
	
	# Stop MQTT loop and disconnect
	mqtt_client.loop_stop()
	mqtt_client.disconnect()