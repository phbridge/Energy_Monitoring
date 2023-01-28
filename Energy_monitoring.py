# https://developer.wifiplug.io/#authentication

import logging.handlers             # Needed for logging
import time                         # Only for time.sleep
import credentials
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
THREAD_TO_BREAK = threading.Event()


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


def master_local_bytes_plugs():
    function_logger = logger.getChild("%s.%s.%s" % (inspect.stack()[2][3], inspect.stack()[1][3], inspect.stack()[0][3]))
    function_logger.info("local_bytes_plugs_thread")
    function_logger.setLevel(logging.INFO)
    historical_upload = ""
    if HOSTS_DB.get("LocalBytes_plugs"):
        while not THREAD_TO_BREAK.is_set():
            now = datetime.now()
            timestamp_string = str(int(now.timestamp()) * 1000000000)
            future = now + timedelta(seconds=30)
            influx_upload = ""
            for each in HOSTS_DB["LocalBytes_plugs"]:
                    url_9 = "http://%s/cm?cmnd=Status+9&user=%s&password=%s" % (HOSTS_DB["LocalBytes_plugs"][each]["address"], HOSTS_DB["LocalBytes_plugs"][each]["username"], HOSTS_DB["LocalBytes_plugs"][each]["password"])
                    url_10 = "http://%s/cm?cmnd=Status+10&user=%s&password=%s" % (HOSTS_DB["LocalBytes_plugs"][each]["address"], HOSTS_DB["LocalBytes_plugs"][each]["username"], HOSTS_DB["LocalBytes_plugs"][each]["password"])
                    PowerLow = 0
                    PowerHigh = 0
                    VoltageLow = 0
                    VoltageHigh = 0
                    CurrentLow = 0
                    CurrentHigh = 0
                    Power = 0
                    ApparentPower = 0
                    ReactivePower = 0
                    Factor = 0
                    Voltage = 0
                    Current = 0
                    power_rate = 0
                    time_slot = datetime.now()
                    function_logger.debug(url_9)
                    function_logger.debug(url_10)
                    try:
                        output_9 = requests.get(url=url_9)
                        output_10 = requests.get(url=url_10)
                        function_logger.debug(output_9.status_code)
                        function_logger.debug(output_10.status_code)
                        function_logger.debug(output_9.json())
                        function_logger.debug(output_10.json())

                        if output_9.status_code == 200:
                            out9_json = output_9.json()
                            PowerLow = out9_json["StatusPTH"]["PowerLow"]
                            PowerHigh = out9_json["StatusPTH"]["PowerHigh"]
                            VoltageLow = out9_json["StatusPTH"]["VoltageLow"]
                            VoltageHigh = out9_json["StatusPTH"]["VoltageHigh"]
                            CurrentLow = out9_json["StatusPTH"]["CurrentLow"]
                            CurrentHigh = out9_json["StatusPTH"]["CurrentHigh"]
                        if output_10.status_code == 200:
                            out10_json = output_10.json()
                            Total = out10_json["StatusSNS"]["ENERGY"]["Total"]
                            Power = out10_json["StatusSNS"]["ENERGY"]["Power"]
                            ApparentPower = out10_json["StatusSNS"]["ENERGY"]["ApparentPower"]
                            ReactivePower = out10_json["StatusSNS"]["ENERGY"]["ReactivePower"]
                            Factor = out10_json["StatusSNS"]["ENERGY"]["Factor"]
                            Voltage = out10_json["StatusSNS"]["ENERGY"]["Voltage"]
                            Current = out10_json["StatusSNS"]["ENERGY"]["Current"]
                            if not HOSTS_DB["LocalBytes_plugs"][each].get("last_power"):
                                HOSTS_DB["LocalBytes_plugs"][each]["last_power"] = out10_json["StatusSNS"]["ENERGY"]["Total"]
                            difference_in_power = out10_json["StatusSNS"]["ENERGY"]["Total"] - HOSTS_DB["LocalBytes_plugs"][each]["last_power"]
                            if not HOSTS_DB["LocalBytes_plugs"][each].get("last_power_time"):
                                HOSTS_DB["LocalBytes_plugs"][each]["last_power_time"] = time_slot
                            difference_in_time = (time_slot - HOSTS_DB["LocalBytes_plugs"][each]["last_power_time"]).seconds
                            HOSTS_DB["LocalBytes_plugs"][each]["last_power_time"] = time_slot
                            power_rate = difference_in_power / (int(difference_in_time) / 3600)
                            HOSTS_DB["LocalBytes_plugs"][each]["last_power"] = out10_json["StatusSNS"]["ENERGY"]["Total"]
                    except requests.exceptions.ConnectionError as e:
                        function_logger.error("ConnectionError %s connecting to %s" % (e, each))
                    except requests.exceptions.Timeout as e:
                        function_logger.error("Timeout %s connecting to %s" % (e, each))
                    except Exception as e:
                        function_logger.error("something went bad connecting to %s" % each)
                        function_logger.error("Unexpected error:%s" % str(sys.exc_info()[0]))
                        function_logger.error("Unexpected error:%s" % str(e))
                        function_logger.error("TRACEBACK=%s" % str(traceback.format_exc()))
                    # cost = Power * PRICE_KWH
                    cost = power_rate * PRICE_KWH

                    influx_upload += "LocalBytes_plugs,plug_name=%s " \
                                     "PowerLow=%s,PowerHigh=%s,VoltageLow=%s,VoltageHigh=%s,CurrentLow=%s,CurrentHigh=%s," \
                                     "Power=%s,ApparentPower=%s,ReactivePower=%s,Factor=%s,Voltage=%s,Current=%s," \
                                     "cost=%s,power_rate=%s \n" % \
                                     (each,
                                      PowerLow, PowerHigh, VoltageLow, VoltageHigh, CurrentLow, CurrentHigh,
                                      Power, ApparentPower, ReactivePower, Factor, Voltage, Current,
                                      cost, power_rate)
            to_send = ""
            for each in influx_upload.splitlines():
                to_send += each + " " + timestamp_string + "\n"
            if not historical_upload == "":
                function_logger.debug("adding history to upload")
                to_send += historical_upload
            if update_influx(to_send):
                historical_upload = ""
            else:
                max_lines = 100
                line_number = 0
                historical_upload = ""
                for line in to_send.splitlines():
                    if line_number < max_lines:
                        historical_upload += line + "\n"
                        line_number += 1
            print(influx_upload)
            time_to_sleep = (future - datetime.now()).seconds
            if 30 > time_to_sleep > 0:
                THREAD_TO_BREAK.wait(time_to_sleep)
    else:
        return



def graceful_killer(signal_number, frame):
    function_logger = logger.getChild("%s.%s.%s" % (inspect.stack()[2][3], inspect.stack()[1][3], inspect.stack()[0][3]))
    function_logger.info("Got Kill signal")
    # api.messages.create(WXT_ALARMS_BOT_ROOM_ID, text=str("CiscoEoL-Bot gracefull process restart requested"))
    function_logger.info('Received:' + str(signal_number))
    THREAD_TO_BREAK.set()
    function_logger.info("set threads to break")
    thread_local_bytes_plugs.join()
    # stats_thread.join()
    # function_logger.info("joined threads")
    # http_server.stop()
    function_logger.info("stopped HTTP server")
    # api.messages.create(WXT_ALARMS_BOT_ROOM_ID, text=str("CiscoEoL-Bot gracefull process restart started"))
    quit()


if __name__ == '__main__':
    # Create Logger
    logger = logging.getLogger("Python_Monitor")
    logger_handler = logging.handlers.TimedRotatingFileHandler(LOGFILE, backupCount=30, when='D')
    logger_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(process)d:%(thread)d:%(name)s - %(message)s')
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

    thread_local_bytes_plugs = threading.Thread(target=lambda: master_local_bytes_plugs())
    thread_local_bytes_plugs.start()
