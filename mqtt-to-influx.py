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
import time
# from datetime import timedelta          # calculate x time ago
# from datetime import datetime           # timestamps mostly


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
                except requests.exceptions.ReadTimeout as e:
                    attempts += 1
                    function_logger.debug("update_influx - attempted " + str(attempts) + " Failed Read Timeout")
                    function_logger.debug("update_influx - Unexpected error:" + str(sys.exc_info()[0]))
                    function_logger.debug("update_influx - Unexpected error:" + str(e))
                    function_logger.debug("update_influx - String was:" + str(string_to_upload).splitlines()[0])
                    function_logger.debug("update_influx - TRACEBACK=" + str(traceback.format_exc()))
                    attempt_error_array.append(str(sys.exc_info()[0]))
                except Exception as e:
                    attempts += 1
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
    function_logger.debug(msg.topic + " " + str(msg.payload))
    function_logger.debug(msg.topic + " " + str(bytes(msg.payload)))
    topic_tree = str(msg.topic).split("/")
    time_slot = datetime.datetime.now()

    def _process_fan_data(serial, message):
        json_message = json.loads(message)
        fan_speed_mapping = {"0000": 2.2,
                             "0001": 4.8,
                             "0002": 5.4,
                             "0003": 6.4,
                             "0004": 7.0,
                             "0005": 10.8,
                             "0006": 14.6,
                             "0007": 20.3,
                             "0008": 27.5,
                             "0009": 37.0,
                             "0010": 46.2
                             }
        function_logger.debug(json_message)
        if json_message["msg"] == "ENVIRONMENTAL-CURRENT-SENSOR-DATA":
            global HOSTS_DB
            HOSTS_DB["DysonFans"][serial]["tact"] = str(round((int(json_message["data"]["tact"])/10) - 273.15, 2))
            HOSTS_DB["DysonFans"][serial]["hact"] = json_message["data"]["hact"]
            HOSTS_DB["DysonFans"][serial]["pm25"] = json_message["data"]["pm25"]
            HOSTS_DB["DysonFans"][serial]["pm10"] = json_message["data"]["pm10"]
            HOSTS_DB["DysonFans"][serial]["va10"] = json_message["data"]["va10"]
            HOSTS_DB["DysonFans"][serial]["noxl"] = json_message["data"]["noxl"]
            HOSTS_DB["DysonFans"][serial]["p25r"] = json_message["data"]["p25r"]
            HOSTS_DB["DysonFans"][serial]["p10r"] = json_message["data"]["p10r"]
            string = "AirQuality,fan=%s,serial=%s " \
                     "temp=%s,humidity=%s," \
                     "pm25=%s,pm10=%s," \
                     "voc=%s,nox=%s," \
                     "p25r=%s,p10r=%s" % \
                     (HOSTS_DB["DysonFans"][serial]["name"], serial,
                      str(round((int(json_message["data"]["tact"])/10) - 273.15, 2)), json_message["data"]["hact"],
                      json_message["data"]["pm25"], json_message["data"]["pm10"],
                      json_message["data"]["va10"], json_message["data"]["noxl"],
                      json_message["data"]["p25r"], json_message["data"]["p10r"])


            return string
        elif json_message["msg"] == "CURRENT-STATE":
            string = "FanPower,fan=%s,serial=%s " \
                     "fanspeed=%s,fanpower=%s," \
                     "cost=%s" % \
                     (HOSTS_DB["DysonFans"][serial]["name"], serial,
                      json_message["product-state"]["fnsp"], fan_speed_mapping[json_message["product-state"]["fnsp"]],
                      str(fan_speed_mapping[json_message["product-state"]["fnsp"]] * (PRICE_KWH/1000))
                      )
            return string
        elif json_message["msg"] == "STATE-CHANGE":
            string = "FanPower,fan=%s,serial=%s " \
                     "fanspeed=%s,fanpower=%s," \
                     "cost=%s" % \
                     (HOSTS_DB["DysonFans"][serial]["name"], serial,
                      json_message["product-state"]["fnsp"],
                      fan_speed_mapping[json_message["product-state"]["fnsp"]],
                      str(fan_speed_mapping[json_message["product-state"]["fnsp"]] * (PRICE_KWH / 1000))
                      )
            return string

    def _process_plug_power(plugname, message_type, message):
        if message_type == "STATE":
            STATE_json = json.loads(message)
            # function_logger.info(out11_json)
            UptimeSec = STATE_json["UptimeSec"]
            Heap = STATE_json["Heap"]
            influx_upload = "LocalBytes_plugs,plug_name=%s " \
                            "uptimesec=%s,Heap=%s \n" % \
                            (plugname, UptimeSec, Heap)
            return influx_upload
        elif message_type == "SENSOR":
            SENSOR_json = json.loads(message)
            # function_logger.info(out11_json)
            Total = SENSOR_json["ENERGY"]["Total"]
            Power = SENSOR_json["ENERGY"]["Power"]
            ApparentPower = SENSOR_json["ENERGY"]["ApparentPower"]
            ReactivePower = SENSOR_json["ENERGY"]["ReactivePower"]
            Factor = SENSOR_json["ENERGY"]["Factor"]
            Voltage = SENSOR_json["ENERGY"]["Voltage"]
            Current = SENSOR_json["ENERGY"]["Current"]
            ReactivePower = SENSOR_json["ENERGY"]["ReactivePower"]
            if not HOSTS_DB["LocalBytes_plugs"][plugname].get("last_power"):
                HOSTS_DB["LocalBytes_plugs"][plugname]["last_power"] = SENSOR_json["ENERGY"]["Total"]
            difference_in_power = SENSOR_json["ENERGY"]["Total"] - HOSTS_DB["LocalBytes_plugs"][plugname]["last_power"]
            if difference_in_power < 0:
                difference_in_power = 0
            if not HOSTS_DB["LocalBytes_plugs"][plugname].get("last_power_time"):
                HOSTS_DB["LocalBytes_plugs"][plugname]["last_power_time"] = time_slot
            difference_in_time = (time_slot - HOSTS_DB["LocalBytes_plugs"][plugname]["last_power_time"]).seconds
            HOSTS_DB["LocalBytes_plugs"][plugname]["last_power_time"] = time_slot
            if difference_in_time > 0:
                power_rate = difference_in_power / (int(difference_in_time) / 3600)
            else:
                power_rate = 0
            HOSTS_DB["LocalBytes_plugs"][plugname]["last_power"] = SENSOR_json["ENERGY"]["Total"]

            cost = power_rate * PRICE_KWH
            cost_absolute = Power * (PRICE_KWH/1000)

            influx_upload = "LocalBytes_plugs,plug_name=%s " \
                            "Power=%s,ApparentPower=%s,ReactivePower=%s,Factor=%s,Voltage=%s,Current=%s,cost=%s,cost_absolute=%s,power_rate=%s \n" % \
                            (plugname, Power, ApparentPower, ReactivePower, Factor, Voltage, Current, cost, cost_absolute, power_rate)
            return influx_upload
        # elif message_type == "STATUS9":
        #     out9_json = json.loads(message)
        #     PowerLow = out9_json["StatusPTH"]["PowerLow"]
        #     PowerHigh = out9_json["StatusPTH"]["PowerHigh"]
        #     VoltageLow = out9_json["StatusPTH"]["VoltageLow"]
        #     VoltageHigh = out9_json["StatusPTH"]["VoltageHigh"]
        #     CurrentLow = out9_json["StatusPTH"]["CurrentLow"]
        #     CurrentHigh = out9_json["StatusPTH"]["CurrentHigh"]
        #     influx_upload = "LocalBytes_plugs,plug_name=%s " \
        #                      "PowerLow=%s,PowerHigh=%s,VoltageLow=%s,VoltageHigh=%s,CurrentLow=%s,CurrentHigh=%s \n" % \
        #                      (plugname,PowerLow, PowerHigh, VoltageLow, VoltageHigh, CurrentLow, CurrentHigh,)
        #     return influx_upload
        # elif message_type == "STATUS10":
        #     out10_json = json.loads(message)
        #     Total = out10_json["StatusSNS"]["ENERGY"]["Total"]
        #     Power = out10_json["StatusSNS"]["ENERGY"]["Power"]
        #     ApparentPower = out10_json["StatusSNS"]["ENERGY"]["ApparentPower"]
        #     ReactivePower = out10_json["StatusSNS"]["ENERGY"]["ReactivePower"]
        #     Factor = out10_json["StatusSNS"]["ENERGY"]["Factor"]
        #     Voltage = out10_json["StatusSNS"]["ENERGY"]["Voltage"]
        #     Current = out10_json["StatusSNS"]["ENERGY"]["Current"]
        #     if not HOSTS_DB["LocalBytes_plugs"][plugname].get("last_power"):
        #         HOSTS_DB["LocalBytes_plugs"][plugname]["last_power"] = out10_json["StatusSNS"]["ENERGY"]["Total"]
        #     difference_in_power = out10_json["StatusSNS"]["ENERGY"]["Total"] - HOSTS_DB["LocalBytes_plugs"][plugname][
        #         "last_power"]
        #     if not HOSTS_DB["LocalBytes_plugs"][plugname].get("last_power_time"):
        #         HOSTS_DB["LocalBytes_plugs"][plugname]["last_power_time"] = time_slot
        #     difference_in_time = (time_slot - HOSTS_DB["LocalBytes_plugs"][plugname]["last_power_time"]).seconds
        #     HOSTS_DB["LocalBytes_plugs"][plugname]["last_power_time"] = time_slot
        #     if difference_in_time > 0:
        #         power_rate = difference_in_power / (int(difference_in_time) / 3600)
        #     else:
        #         power_rate = 0
        #     HOSTS_DB["LocalBytes_plugs"][plugname]["last_power"] = out10_json["StatusSNS"]["ENERGY"]["Total"]
        #
        #     cost = power_rate * PRICE_KWH
        #
        #     influx_upload = "LocalBytes_plugs,plug_name=%s " \
        #                      "Power=%s,ApparentPower=%s,ReactivePower=%s,Factor=%s,Voltage=%s,Current=%s,cost=%s,power_rate=%s \n" % \
        #                      (plugname, Power, ApparentPower, ReactivePower, Factor, Voltage, Current, cost, power_rate)
        #     return influx_upload
        # elif message_type == "STATUS11":
        #     out11_json = json.loads(message)
        #     # function_logger.info(out11_json)
        #     UptimeSec = out11_json["StatusSTS"]["UptimeSec"]
        #     Heap = out11_json["StatusSTS"]["Heap"]
        #     influx_upload = "LocalBytes_plugs,plug_name=%s " \
        #                      "uptimesec=%s,Heap=%s \n" % \
        #                      (plugname, UptimeSec, Heap)
        #     return influx_upload
        else:
            function_logger.info("got unrecognied or unwanted PLUG message:%s topic:%s from:%s" % (message, str(topic_tree), plugname))

    try:
        # if topic_tree[0] == "energy":  # this will likly go plug when topics updated
        #     if topic_tree[1] == "plugpower":
        #         influx_upload = proceess_plug_power(topic_tree[2], topic_tree[3], msg.payload.decode('utf-8'))
        #         function_logger.debug(influx_upload)
        #         if update_influx(influx_upload):
        #             function_logger.debug(influx_upload)
        #         else:
        #             function_logger.error("influx upload failed")

        if topic_tree[0] == "tele":  # this mostly handels the plugs
            if topic_tree[1] in HOSTS_DB["LocalBytes_plugs"].keys():
                influx_upload = _process_plug_power(plugname=topic_tree[1], message_type=topic_tree[2], message=msg.payload.decode('utf-8'))
                function_logger.debug(influx_upload)
                if update_influx(influx_upload):
                    function_logger.debug(influx_upload)
                else:
                    function_logger.error("influx upload failed")
            else:
                function_logger.warning("got message from unauthorised plug topic:%s message:%s" % (msg.topic, str(msg.payload)))
        elif topic_tree[0] == "438":
            if topic_tree[1] in HOSTS_DB["DysonFans"].keys():
                if topic_tree[2] == "status":
                    if topic_tree[3] == "current":
                        influx_upload = _process_fan_data(serial=topic_tree[1], message=msg.payload.decode('utf-8'))
                        function_logger.info(influx_upload)
                        if update_influx(influx_upload):
                            function_logger.debug(influx_upload)
                        else:
                            function_logger.error("influx upload failed")
            else:
                function_logger.warning("got message from unauthorised Fan topic:%s message:%s" % (msg.topic, str(msg.payload)))
        else:
            function_logger.info("got unrecognied or unwanted message topic:%s message:%s" % (msg.topic, str(msg.payload)))
    except IndexError as e:
        function_logger.error("IndexError recieving message")
        function_logger.error("IndexError recieving message topic:%s message:%s" % (msg.topic, str(msg.payload)))
        function_logger.error("Unexpected error:" + str(sys.exc_info()[0]))
        function_logger.error("Unexpected error:" + str(e))
        function_logger.error("TRACEBACK=" + str(traceback.format_exc()))
    except Exception as e:
        function_logger.error("something went bad recieving message")
        function_logger.error("Unexpected error:" + str(sys.exc_info()[0]))
        function_logger.error("Unexpected error:" + str(e))
        function_logger.error("TRACEBACK=" + str(traceback.format_exc()))


def graceful_killer(signal_number, frame):
    function_logger = logger.getChild("%s.%s.%s" % (inspect.stack()[2][3], inspect.stack()[1][3], inspect.stack()[0][3]))
    function_logger.info("Got Kill signal")
    function_logger.info('Received:' + str(signal_number))
    THREAD_TO_BREAK.set()
    request_fan_data.join()
    function_logger.info("set threads to break")


def request_fan_data_thread():
    function_logger = logger.getChild("%s.%s" % (inspect.stack()[1][3], inspect.stack()[0][3]))
    function_logger.debug("starting equest_fan_data_thread")

    def _mqtt_time():
        """Return current time string for mqtt messages."""
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def _request_fan_data(serial):
        try:
            future_speed = 0
            if HOSTS_DB["DysonFans"][serial]["pm25"] > 250.5 or HOSTS_DB["DysonFans"][serial]["pm10"] > 420.5 or HOSTS_DB["DysonFans"][serial]["va10"] > 8.5 or HOSTS_DB["DysonFans"][serial]["noxl"] > 8.5:
                future_speed = 9  # this is the "RED" level for most
            elif HOSTS_DB["DysonFans"][serial]["pm25"] > 150.5 or HOSTS_DB["DysonFans"][serial]["pm10"] > 350.5:
                future_speed += 4
            elif HOSTS_DB["DysonFans"][serial]["pm25"] > 70.5 or HOSTS_DB["DysonFans"][serial]["pm10"] > 100.5:
                future_speed += 3
            elif HOSTS_DB["DysonFans"][serial]["pm25"] > 53.5 or HOSTS_DB["DysonFans"][serial]["pm10"] > 75.5 or HOSTS_DB["DysonFans"][serial]["va10"] > 6.5 or HOSTS_DB["DysonFans"][serial]["noxl"] > 6.5:
                future_speed += 2 # this is the "ORANGE" level for most
            elif HOSTS_DB["DysonFans"][serial]["pm25"] > 35.5 or HOSTS_DB["DysonFans"][serial]["pm10"] > 50.5 or HOSTS_DB["DysonFans"][serial]["va10"] > 3.5 or HOSTS_DB["DysonFans"][serial]["noxl"] > 3.5:
                future_speed += 1  # this is the "YELLOW" level for most
            else:
                future_speed = 1  # this is the "GREEN" level for most

            if not HOSTS_DB["DysonFans"][serial]["current_speed"] == future_speed:
                HOSTS_DB["DysonFans"][serial]["device_object"].set_speed = future_speed

            function_logger.critical("%s - %s - %s" % (HOSTS_DB["DysonFans"][serial]["name"], HOSTS_DB["DysonFans"][serial]["current_speed"], future_speed))
        except IndexError:
            function_logger.critical("no data yet for fan writing zero")
            global HOSTS_DB
            HOSTS_DB["DysonFans"][serial]["tact"] = 0
            HOSTS_DB["DysonFans"][serial]["hact"] = 0
            HOSTS_DB["DysonFans"][serial]["pm25"] = 0
            HOSTS_DB["DysonFans"][serial]["pm10"] = 0
            HOSTS_DB["DysonFans"][serial]["va10"] = 0
            HOSTS_DB["DysonFans"][serial]["noxl"] = 0
            HOSTS_DB["DysonFans"][serial]["p25r"] = 0
            HOSTS_DB["DysonFans"][serial]["p10r"] = 0

        function_logger.debug("requesting fan data for %s" % serial)
        topic = "438/%s/command" % serial
        payload = {
            "msg": "REQUEST-PRODUCT-ENVIRONMENT-CURRENT-SENSOR-DATA",
            "time": _mqtt_time(),
        }
        mqttc.publish(topic=topic, payload=json.dumps(payload))
        payload = {
            "msg": "REQUEST-CURRENT-STATE",
            "time": _mqtt_time(),
        }
        mqttc.publish(topic=topic, payload=json.dumps(payload))
        return

    time.sleep(45)
    while not THREAD_TO_BREAK.is_set():
        start_time = datetime.datetime.now()
        for serial in HOSTS_DB["DysonFans"].keys():
            _request_fan_data(serial=serial)
        next_time = start_time + datetime.timedelta(seconds=30)
        function_logger.debug("sleeping for %s seconds" % (next_time - start_time).seconds)
        if (next_time - start_time).seconds > 0:
            THREAD_TO_BREAK.wait((next_time - start_time).seconds)


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
    logger.info("__main__ - " + "Python Energy Monitor Logger")

    # Catch SIGTERM etc
    signal.signal(signal.SIGHUP, graceful_killer)
    signal.signal(signal.SIGTERM, graceful_killer)

    # GET_CURRENT_DB
    logger.info("__main__ - " + "GET_CURRENT_DB")
    HOSTS_DB = load_hosts_file_json()
    #

    ## MQTT logic - Register callbacks and start MQTT client
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message

    # start sending data requests every 30 seconds
    request_fan_data = threading.Thread(target=lambda: request_fan_data_thread())
    request_fan_data.start()

    while not THREAD_TO_BREAK.is_set():
        mqttc.loop()

    mqttc.disconnect()

