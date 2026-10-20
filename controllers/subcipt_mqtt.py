import json
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import socket
from datetime import datetime
import uuid

from config import opcional


class MQTTHandler:
    def __init__(self, broker_host=None, broker_port=None):
        broker_host = broker_host or opcional('MQTT_BROKER_HOST', '10.162.239.90')
        broker_port = broker_port or int(opcional('MQTT_BROKER_PORT', '1883'))
        self.client = mqtt.Client(callback_api_version = CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
        self.device_id = f"validar_image_{socket.gethostname()}_{uuid.uuid4().hex[:8]}"
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        try:
            self.client.connect_async(broker_host, broker_port, 60)
            self.client.loop_start()
        except Exception as e:
            print(f" No se pudo iniciar la conexión MQTT: {e}")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(" Conectado al broker MQTT")
        else:
            print(f" Error de conexión: {rc}")

    def on_message(self, client, userdata, msg):
        print(f" Mensaje en '{msg.topic}': {msg.payload.decode()}")

    def enviar_mensaje(self, tabla, mensaje, tipo="info"):
        data = {
            "timestamp": datetime.now().isoformat(),
            "tabla": tabla,
            "type": tipo,
            "message": mensaje
        }
        if tabla == 'EMERGENCIA':
            topic = 'alerta/FS40/scaneo'
        else:
            topic = f"alerta/{tabla}"
            print(f" Enviado → {topic}")
            
        self.client.publish(topic, json.dumps(data))
        


    
# Uso del handler
mqtt_handler = MQTTHandler()

# # Ejemplo: enviar mensaje
# mqtt_handler.enviar_mensaje("lidar", "Error de conexión", "error")

# mqtt