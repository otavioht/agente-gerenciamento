import time
# import paho.mqtt.client as mqtt
from gmqtt import Client as MQTTClient
import requests
import psutil
import json
import os
import asyncio
import random
import logging

from urllib import parse
from urllib.parse import urlparse
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from threading import Thread

from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device.aio import ProvisioningDeviceClient
from azure.iot.device import Message, MethodResponse

app = Flask(__name__)

AGENT_TIME_INTERVAL = 300 #Intervalo em segundos

# Configurações do Broker local
BROKER_ADDRESS = "localhost"  # Endereço do broker Mosquitto
# BROKER_ADDRESS = "20.120.0.64"  # Endereço do broker Mosquitto
BROKER_PORT = 1883  # Porta padrão para MQTT
BROKER_TIMEOUT = 60  # Timeout em segundos


PROVISIONING_HOST = "global.azure-devices-provisioning.net"
ID_SCOPE = os.getenv("ID_SCOPE")  # DPS ID Scope
REGISTRATION_ID = os.getenv("AGENT_DEVICE_ID")  # Device ID
ESP_SYMMETRIC_KEY = os.getenv("ESP_DEVICE_KEY")  # Device Key
AGENT_SYMMETRIC_KEY = os.getenv("AGENT_DEVICE_KEY")  # Device Key
ESP_MODEL_ID = os.getenv("ESP_MODEL_ID")  # Device Key
AGENT_MODEL_ID = os.getenv("AGENT_MODEL_ID")  # Device Key
# GLOBAL THERMOSTAT VARIABLES
usoCPU = None
usoMemoria = None
usoRede = None
connectedDevices = None


def get_network_throughput():
    old_value = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
    time.sleep(1)
    new_value = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
    return (new_value - old_value)/1000

# PROVISION DEVICE

async def provision_device(provisioning_host, id_scope, registration_id, symmetric_key, model_id):

    provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
        provisioning_host=provisioning_host,
        registration_id=registration_id,
        id_scope=id_scope,
        symmetric_key=symmetric_key,
    )

    provisioning_device_client.provisioning_payload = {"modelId": model_id}
    return await provisioning_device_client.register()

async def execute_command_listener(
    device_client, method_name, user_command_handler, create_user_response_handler
):
    while True:
        if method_name:
            command_name = method_name
        else:
            command_name = None

        command_request = await device_client.receive_method_request(command_name)
        print("Command request received with payload")
        print(command_request.payload)

        values = {}
        if not command_request.payload:
            print("Payload was empty.")
        else:
            values = command_request.payload

        await user_command_handler(values)

        response_status = 200
        response_payload = create_user_response_handler(values)

        command_response = MethodResponse.create_from_method_request(
            command_request, response_status, response_payload
        )

        try:
            await device_client.send_method_response(command_response)
        except Exception:
            print("responding to the {command} command failed".format(command=method_name))

async def execute_property_listener(device_client):
    ignore_keys = ["__t", "$version"]
    while True:
        patch = await device_client.receive_twin_desired_properties_patch()  # blocking call

        print("the data in the desired properties patch was: {}".format(patch))

        version = patch["$version"]
        prop_dict = {}

        for prop_name, prop_value in patch.items():
            if prop_name in ignore_keys:
                continue
            else:
                prop_dict[prop_name] = {
                    "ac": 200,
                    "ad": "Successfully executed patch",
                    "av": version,
                    "value": prop_value,
                }

        await device_client.patch_twin_reported_properties(prop_dict)

async def send_telemetry_msg(device_client, telemetry_msg):
    msg = Message(json.dumps(telemetry_msg))
    msg.content_encoding = "utf-8"
    msg.content_type = "application/json"
    print("Sent message")
    print(msg)
    await device_client.send_message(msg)


# MAIN STARTS
async def main():
    # Send telemetry
    client2 = MQTTClient("client2")
    await client2.connect(BROKER_ADDRESS, BROKER_PORT)
    client2.subscribe("esp32/+/+")
    client2.subscribe("connected_devices")

    async def send_telemetry():
        print("Sending telemetry for performance")
        global usoCPU
        global usoMemoria
        global usoRede
        global connectedDevices
        usoCPU = get_cpu_usage()
        usoMemoria = get_memory_usage()
        usoRede = get_network_throughput()
        data = {}
        data['usoRede'] = usoRede
        data['usoCPU'] = usoCPU
        data['usoMemoria'] = usoMemoria
        data['dispositivosConectados'] = connectedDevices
        await send_telemetry_msg(device_client, data)
        connectedDevices = 0
        client2.publish("connected_devices", 'GET', qos=1)



    registration_result = await provision_device(
        PROVISIONING_HOST, ID_SCOPE, REGISTRATION_ID, AGENT_SYMMETRIC_KEY, AGENT_MODEL_ID
    )

    if registration_result.status == "assigned":
        print("Device was assigned")
        print(registration_result.registration_state.assigned_hub)
        print(registration_result)
        print(registration_result.registration_state.device_id)

        device_client = IoTHubDeviceClient.create_from_symmetric_key(
            symmetric_key=AGENT_SYMMETRIC_KEY,
            hostname=registration_result.registration_state.assigned_hub,
            device_id=registration_result.registration_state.device_id,
            product_info=AGENT_MODEL_ID,
        )
    else:
        raise RuntimeError(
            "Could not provision device. Aborting Plug and Play device connection."
        )


    # Connect the client.
    await device_client.connect()

    ################################################
    # Set and read desired property (target temperature)

    # await device_client.patch_twin_reported_properties({"maxTempSinceLastReboot": max_temp})

    ################################################
    # Register callback and Handle command (reboot)
    # print("Listening for command requests and property updates")

    # listeners = asyncio.gather(
    #     execute_command_listener(
    #         device_client,
    #         method_name="reboot",
    #         user_command_handler=reboot_handler,
    #         create_user_response_handler=create_reboot_response,
    #     ),
    #     execute_command_listener(
    #         device_client,
    #         method_name="getMaxMinReport",
    #         user_command_handler=max_min_handler,
    #         create_user_response_handler=create_max_min_report_response,
    #     ),
    #     execute_property_listener(device_client),
    # )

    ################################################


    # send_telemetry_task = asyncio.create_task(send_telemetry())
    while True:
        await send_telemetry()
        await asyncio.sleep(AGENT_TIME_INTERVAL)  # Change this to 60 seconds
    # send_telemetry_task.print_stack()
    # Run the stdin listener in the event loop
    # loop = asyncio.get_running_loop()

    # user_finished = loop.run_in_executor(None, stdin_listener)
    # # Wait for user to indicate they are done listening for method calls
    # await user_finished

    # if not listeners.done():
    #     listeners.set_result("DONE")

    # listeners.cancel()

    # send_telemetry_task.print_stack()

    # Finally, shut down the client
    # await device_client.shutdown()


# Funções para obter métricas da Raspberry Pi
def get_cpu_usage():
    return psutil.cpu_percent()

def get_memory_usage():
    return psutil.virtual_memory().percent



# Funções para enviar métricas para a nuvem
# def send_metric_to_cloud(device_id, metric_name, value):
#     endpoint = f'https://your_api_endpoint/devices/{device_id}/{metric_name}'
#     payload = {'value': value}
#     response = requests.post(endpoint, json=payload)
#     return response.json()

# def send_metrics_from_pi_to_cloud(cpu, memory, network):
#     payload = {
#         'cpu_usage': cpu,
#         'memory_usage': memory,
#         'network_throughput': network
#     }
#     response = requests.post('https://your_api_endpoint/raspberry_metrics', json=payload)
#     return response.json()

# def get_config_from_cloud():
#     response = requests.get('https://your_api_endpoint/config')
#     return response.json()



# @app.route('/agent/config', methods=['POST'])
# def update_agent_config():
#     data = request.json
#     global AGENT_TIME_INTERVAL
#     AGENT_TIME_INTERVAL = data.get('new_interval', AGENT_TIME_INTERVAL)
#     return jsonify(success=True)

# @app.route('/esp32/config', methods=['POST'])
# def update_esp_config():
#     data = request.json
#     configs = data.get('configs', [])

#     for config_data in configs:
#         config = ESPCONFIG(config_data)
        
#         # Publicar a nova configuração para o tópico correspondente do ESP32
#         topic = f"esp32/{config.device_id}/config"
#         client.publish(topic, json.dumps(config.to_dict()))

#     return jsonify(success=True)

logging.basicConfig(level=logging.ERROR)
# Inicialização do MQTT


# calling the batch send

# batch data as a list of dictionary items
# note: the property 'iothub-app-iothub-creation-time-utc' allows the ingestion time into the hub to be overridden with the supplied UTC ISO-3339 format time stamp
# other custom message properties can be included here as well if needed.  The properties dictionary is optional.

# if __name__ == '__main__':
#     
# def send_connected_devices():
#     global connectedDevices
#     client.publish("connected_devices", connectedDevices)
#     connectedDevices = 0
#     return

async def on_message(client, topic, payload, qos, properties):
    value = payload.decode('utf-8')
    if topic == "connected_devices" and value == 'CONNECTED':
        global connectedDevices
        connectedDevices += 1
        print(connectedDevices)
    elif topic == "connected_devices" and value == 'GET':
        print('GET')
    else:
        print(client, value)
        _, device_id, metric_name = topic.split('/')
        data = {}
        data[metric_name] = value
        registration_result = await provision_device(
            "global.azure-devices-provisioning.net", ID_SCOPE, device_id, ESP_SYMMETRIC_KEY, ESP_MODEL_ID
        )

        if registration_result.status == "assigned":
            print("Device was assigned")
            print(registration_result.registration_state.assigned_hub)
            print(registration_result.registration_state.device_id)

            device_client = IoTHubDeviceClient.create_from_symmetric_key(
                symmetric_key=ESP_SYMMETRIC_KEY,
                hostname=registration_result.registration_state.assigned_hub,
                device_id=registration_result.registration_state.device_id,
                product_info=ESP_MODEL_ID,
            )
        else:
            raise RuntimeError(
                "Could not provision device. Aborting Plug and Play device connection."
            )

        send_telemetry_msg(device_client, data)

async def mqttStart():
    client = MQTTClient("client1")
    client.on_message = on_message
    await client.connect(BROKER_ADDRESS, BROKER_PORT)
    client.subscribe("esp32/+/+")
    client.subscribe("connected_devices")
    # Start the MQTT loop

async def main_coroutine():
    task1 = asyncio.create_task(mqttStart())
    task2 = asyncio.create_task(main())
    await asyncio.gather(task1, task2)

if __name__ == "__main__":
    print('starting asyncio on main')
    
    # Start Flask in a separate thread
    t = Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': 5000, 'threaded': True})
    t.start()
    # MQTT Client Setup

    asyncio.run(main_coroutine())


#     while True:
        # client.loop_start()
        
        # Coleta e envia métricas da Raspberry Pi
        # usoCPU = get_cpu_usage()
        # usoMemoria = get_memory_usage()
        # usoRede = get_network_throughput()
        # response = send_metrics_from_pi_to_cloud(cpu, memory, network)
        # Imprime resposta da API após enviar métricas da Pi
        # print(response)  
        # data = []
        
        # for i in range(1, 5):
        #     data.append(generateDataObject(usoCPU, usoMemoria, usoRede))
        #     print(data)
        #     time.sleep(AGENT_TIME_INTERVAL)
        # if (iot_hub_host != ""):
        #     errorDuringSend = send_batch_data(device_id, iot_hub_host, device_symmetric_key, data)

        #     if (errorDuringSend):
        #         print("The following data was unable to be sent:")
        #         for x in data:
        #             if (x["error"]):
        #                 print("\tdata:{0}".format(x))   
        #     else:
        #         print("Success sending batch")
        # else:
        #     print("Something went wrong with the DPS registration")
        
        # client.loop_stop()

