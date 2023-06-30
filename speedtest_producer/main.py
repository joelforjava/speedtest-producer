import json
import os
import subprocess
import sys
from configparser import ConfigParser
from pathlib import Path

from confluent_kafka import Producer
from rich import print


CONFIG_FILE_NAME = "speedtest-producer.ini"
"""The name of the configuration file. Where you put it is anyone's guess!"""


def load_config():
    config = ConfigParser()
    for loc in Path(), Path.home(), Path("/etc/speedtest-producer"), Path(os.environ.get("SPEEDTEST_PRODUCER_CONF", "..")):
        try: 
            with (loc / CONFIG_FILE_NAME).open("r") as source:
                config.read_file(source)
        except IOError:
            pass
        else:
            print(f"Found config at {loc}")
            print(config["KAFKA"]["topic"])

    return config


CONFIG = load_config()


SPEEDTEST_CMD = CONFIG["DEFAULT"]["speedtest_command"]


BACKUP_FILE_PATH = Path(CONFIG["BACKUP"]["file_path"])
"""The file path used to store the results locally."""


def send_to_kafka(results: str):
    kafka_conf = CONFIG["KAFKA"]
    # TODO - probably could just pass as-is, but need to separate other items, e.g. topic name and key, out
    conf = {
        "bootstrap.brokers": kafka_conf["bootstrap.brokers"],
        "client.id": kafka_conf["client.id"]
    }
    producer = Producer(conf)
    producer.produce(kafka_conf["topic"], key=kafka_conf["key"], value=results)


def write_results(results: str):
    if not BACKUP_FILE_PATH.parent.exists():
        BACKUP_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Saving data to: {BACKUP_FILE_PATH}")
    with BACKUP_FILE_PATH.open("a") as fp:
        fp.write(results)


def call_speedtest():
    # TODO - determine full path!
    cmd_params = [SPEEDTEST_CMD, "--json", "--secure"]
    resp = subprocess.run(cmd_params, capture_output=True, encoding=sys.getdefaultencoding())

    if resp.stderr:
        print(resp.stderr)

    return resp.stdout


def run():
    resp = call_speedtest()

    try:
        results = json.loads(resp)
    except json.JSONDecodeError:
        print("Unable to parse results")
        results = resp

    print(results)

    if CONFIG["KAFKA"].getboolean("enabled"):
        send_to_kafka(resp)
    if CONFIG["BACKUP"].getboolean("enabled"):
        write_results(resp)


if __name__ == "__main__":
    run()
