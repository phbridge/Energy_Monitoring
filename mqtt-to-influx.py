import paho.mqtt.client as mqtt
import logging.handlers             # Needed for logging
import traceback
import sys
import json
import threading
import random
import requests
import datetime
import signal
import subprocess
import inspect
import credentials
import requests
from datetime import timedelta          # calculate x time ago
from datetime import datetime           # timestamps mostly


ABSOLUTE_PATH = credentials.ABSOLUTE_PATH
INFLUX_DB_PATH = credentials.INFLUX_DB_PATH
LOGFILE = credentials.LOGFILE
PRICE_KWH = credentials.PRICE_KWH
MQTT_BROKER_URL = credentials.MQTT_BROKER_URL
MQTT_USERNAME = credentials.MQTT_USERNAME
MQTT_PASSWORD = credentials.MQTT_PASSWORD
MQTT_TOPICS = credentials.MQTT_TOPICS

THREAD_TO_BREAK = threading.Event()

mqttc = mqtt.Client()
mqttc.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqttc.connect(MQTT_BROKER_URL)


def update_influx(raw_string, timestamp=None):
    function_logger = logger.getChild("%s.%s.%s" % (inspect.stack()[2][3], inspect.stack()[1][3], inspect.stack()[0][3]))
    try:
        string_to_upload = ""
        if timestamp is not None:
            timestamp_string = str(int(timestamp.timestamp()) * 1000000000)
            for each in raw_string.splitlines():
                string_to_upload += each + " " + timestamp_string + "\n"
        else:
            string_to_upload = raw_string
        success_array = []
        upload_to_influx_sessions = requests.session()
        for influx_path_url in INFLUX_DB_PATH:
            success = False
            attempts = 0
            attempt_error_array = []
            while attempts < 5 and not success:
                try:
                    upload_to_influx_sessions_response = upload_to_influx_sessions.post(url=influx_path_url, data=string_to_upload, timeout=(20, 10))
                    if upload_to_influx_sessions_response.status_code == 204:
                        function_logger.debug("content=%s" % upload_to_influx_sessions_response.content)
                        function_logger.debug("status_code=%s" % upload_to_influx_sessions_response.status_code)
                        success = True
                    else:
                        attempts += 1
                        function_logger.warning("status_code=%s" % upload_to_influx_sessions_response.status_code)
                        function_logger.warning("content=%s" % upload_to_influx_sessions_response.content)
                except requests.exceptions.ConnectTimeout as e:
                    attempts += 1
                    function_logger.debug("update_influx - attempted " + str(attempts) + " Failed Connection Timeout")
                    function_logger.debug("update_influx - Unexpected error:" + str(sys.exc_info()[0]))
                    function_logger.debug("update_influx - Unexpected error:" + str(e))
                    function_logger.debug("update_influx - String was:" + str(string_to_upload).splitlines()[0])
                    function_logger.debug("update_influx - TRACEBACK=" + str(traceback.format_exc()))
                    attempt_error_array.append(str(sys.exc_info()[0]))
                except requests.exceptions.ConnectionError as e:
                    attempts += 1
                    function_logger.debug("update_influx - attempted " + str(attempts) + " Failed Connection Error")
                    function_logger.debug("update_influx - Unexpected error:" + str(sys.exc_info()[0]))
                    function_logger.debug("update_influx - Unexpected error:" + str(e))
                    function_logger.debug("update_influx - String was:" + str(string_to_upload).splitlines()[0])
                    function_logger.debug("update_influx - TRACEBACK=" + str(traceback.format_exc()))
                    attempt_error_array.append(str(sys.exc_info()[0]))
                except Exception as e:
                    function_logger.error("update_influx - attempted " + str(attempts) + " Failed")
                    function_logger.error("update_influx - Unexpected error:" + str(sys.exc_info()[0]))
                    function_logger.error("update_influx - Unexpected error:" + str(e))
                    function_logger.error("update_influx - String was:" + str(string_to_upload).splitlines()[0])
                    function_logger.debug("update_influx - TRACEBACK=" + str(traceback.format_exc()))
                    attempt_error_array.append(str(sys.exc_info()[0]))
                    break
            success_array.append(success)
        upload_to_influx_sessions.close()
        super_success = False
        for each in success_array:
            if not each:
                super_success = False
                break
            else:
                super_success = True
        if not super_success:
            function_logger.error("update_influx - FAILED after 5 attempts. Failed up update " + str(string_to_upload.splitlines()[0]))
            function_logger.error("update_influx - FAILED after 5 attempts. attempt_error_array: " + str(attempt_error_array))
            return False
        else:
            function_logger.debug("update_influx - " + "string for influx is " + str(string_to_upload))
            function_logger.debug("update_influx - " + "influx status code is  " + str(upload_to_influx_sessions_response.status_code))
            function_logger.debug("update_influx - " + "influx response is code is " + str(upload_to_influx_sessions_response.text[0:1000]))
            return True
    except Exception as e:
        function_logger.error("update_influx - something went bad sending to InfluxDB")
        function_logger.error("update_influx - Unexpected error:" + str(sys.exc_info()[0]))
        function_logger.error("update_influx - Unexpected error:" + str(e))
        function_logger.error("update_influx - TRACEBACK=" + str(traceback.format_exc()))
    return False


def on_connect(client, userdata, flags, rc):
    function_logger = logger.getChild("%s.%s.%s" % (inspect.stack()[2][3], inspect.stack()[1][3], inspect.stack()[0][3]))
    function_logger.info("Connected with result code "+str(rc))
    for topic in MQTT_TOPICS:
        client.subscribe(topic)


def on_message(client, userdata, msg):
    function_logger = logger.getChild("%s.%s.%s" % (inspect.stack()[2][3], inspect.stack()[1][3], inspect.stack()[0][3]))
    print(msg.topic + " " + str(msg.payload))
    # print(msg.topic + " " + str(bytes(msg.payload)))
    topic_tree = str(msg.topic).split("/")
    time_slot = datetime.now()

    def proceess_plug_power(plugname, message_type, message):
        if message_type == "STATUS9":
            out9_json = json.loads(message)
            PowerLow = out9_json["StatusPTH"]["PowerLow"]
            PowerHigh = out9_json["StatusPTH"]["PowerHigh"]
            VoltageLow = out9_json["StatusPTH"]["VoltageLow"]
            VoltageHigh = out9_json["StatusPTH"]["VoltageHigh"]
            CurrentLow = out9_json["StatusPTH"]["CurrentLow"]
            CurrentHigh = out9_json["StatusPTH"]["CurrentHigh"]
            influx_upload = "LocalBytes_plugs,plug_name=%s " \
                             "PowerLow=%s,PowerHigh=%s,VoltageLow=%s,VoltageHigh=%s,CurrentLow=%s,CurrentHigh=%s \n" % \
                             (plugname,PowerLow, PowerHigh, VoltageLow, VoltageHigh, CurrentLow, CurrentHigh,)
            return influx_upload
        elif message_type == "STATUS10":
            out10_json = json.loads(message)
            Total = out10_json["StatusSNS"]["ENERGY"]["Total"]
            Power = out10_json["StatusSNS"]["ENERGY"]["Power"]
            ApparentPower = out10_json["StatusSNS"]["ENERGY"]["ApparentPower"]
            ReactivePower = out10_json["StatusSNS"]["ENERGY"]["ReactivePower"]
            Factor = out10_json["StatusSNS"]["ENERGY"]["Factor"]
            Voltage = out10_json["StatusSNS"]["ENERGY"]["Voltage"]
            Current = out10_json["StatusSNS"]["ENERGY"]["Current"]
            if not HOSTS_DB["LocalBytes_plugs"][plugname].get("last_power"):
                HOSTS_DB["LocalBytes_plugs"][plugname]["last_power"] = out10_json["StatusSNS"]["ENERGY"]["Total"]
            difference_in_power = out10_json["StatusSNS"]["ENERGY"]["Total"] - HOSTS_DB["LocalBytes_plugs"][plugname][
                "last_power"]
            if not HOSTS_DB["LocalBytes_plugs"][plugname].get("last_power_time"):
                HOSTS_DB["LocalBytes_plugs"][plugname]["last_power_time"] = time_slot
            difference_in_time = (time_slot - HOSTS_DB["LocalBytes_plugs"][plugname]["last_power_time"]).seconds
            HOSTS_DB["LocalBytes_plugs"][plugname]["last_power_time"] = time_slot
            if difference_in_time > 0:
                power_rate = difference_in_power / (int(difference_in_time) / 3600)
            else:
                power_rate = 0
            HOSTS_DB["LocalBytes_plugs"][plugname]["last_power"] = out10_json["StatusSNS"]["ENERGY"]["Total"]

            cost = power_rate * PRICE_KWH

            influx_upload = "LocalBytes_plugs,plug_name=%s " \
                             "Power=%s,ApparentPower=%s,ReactivePower=%s,Factor=%s,Voltage=%s,Current=%s,cost=%s,power_rate=%s \n" % \
                             (plugname, Power, ApparentPower, ReactivePower, Factor, Voltage, Current, cost, power_rate)
            return influx_upload
        elif message_type == "STATUS11":
            out11_json = json.loads(message)
            # function_logger.info(out11_json)
            UptimeSec = out11_json["StatusSTS"]["UptimeSec"]
            Heap = out11_json["StatusSTS"]["Heap"]
            influx_upload = "LocalBytes_plugs,plug_name=%s " \
                             "uptimesec=%s,Heap=%s \n" % \
                             (plugname, UptimeSec, Heap)
            return influx_upload
        else:
            function_logger.info("got unrecognied or unwanted message:%s" % message)

    if topic_tree[0] == "energy":
        if topic_tree[1] == "plugpower":
            influx_upload = proceess_plug_power(topic_tree[2], topic_tree[3], msg.payload.decode('utf-8'))
            update_influx(influx_upload)
    else:
        function_logger.info("got unrecognied or unwanted message topic:%s message:%s" % (msg.topic, str(msg.payload)))


def graceful_killer(signal_number, frame):
    function_logger = logger.getChild("%s.%s.%s" % (inspect.stack()[2][3], inspect.stack()[1][3], inspect.stack()[0][3]))
    function_logger.info("Got Kill signal")
    function_logger.info('Received:' + str(signal_number))
    THREAD_TO_BREAK.set()
    function_logger.info("set threads to break")
    quit()


def load_hosts_file_json():
    function_logger = logger.getChild("%s.%s" % (inspect.stack()[1][3], inspect.stack()[0][3]))
    try:
        function_logger.debug("opening host file")
        user_filename = ABSOLUTE_PATH + "hosts.json"
        with open(user_filename) as host_json_file:
            return_db_json = json.load(host_json_file)
        function_logger.debug("closing host file")
        function_logger.debug("HOSTS_JSON=%s" % str(return_db_json))
        function_logger.info("host Records=%s" % str(len(return_db_json)))
        function_logger.debug("HOSTS=%s" % str(return_db_json.keys()))
        return return_db_json
    except Exception as e:
        function_logger.error("something went bad opening host file")
        function_logger.error("Unexpected error:%s" % str(sys.exc_info()[0]))
        function_logger.error("Unexpected error:%s" % str(e))
        function_logger.error("TRACEBACK=%s" % str(traceback.format_exc()))
    return {}


if __name__ == '__main__':
    # Create Logger
    logger = logging.getLogger("Python_Monitor")
    logger_handler = logging.handlers.TimedRotatingFileHandler(LOGFILE, backupCount=30, when='D')
    logger_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(process)d:%(thread)d:%(name)s - %(message)s')
    logger_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_handler)
    logger.setLevel(logging.INFO)
    logger.info("---------------------- STARTING ----------------------")
    logger.info("__main__ - " + "Python Monitor Logger")

    # Catch SIGTERM etc
    signal.signal(signal.SIGHUP, graceful_killer)
    signal.signal(signal.SIGTERM, graceful_killer)

    # GET_CURRENT_DB
    logger.info("__main__ - " + "GET_CURRENT_DB")
    HOSTS_DB = load_hosts_file_json()
    #
    # thread_local_bytes_plugs = threading.Thread(target=lambda: master_local_bytes_plugs())
    # thread_local_bytes_plugs.start()
    ## MQTT logic - Register callbacks and start MQTT client
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.loop_forever()


