import json
import os
import subprocess
import sys
from configparser import ConfigParser
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.error import ProduceError
from rich import print


POSSIBLE_CONFIG_LOCATIONS = (
    Path(),
    Path.home(),
    Path("/etc/speedtest-producer"),
    Path("/opt/speedtest-producer"),
    Path(os.environ.get("SPEEDTEST_PRODUCER_CONF", "..")),
)
"""All possible locations to search for the configuration file."""


CONFIG_FILE_NAME = "speedtest-producer.ini"
"""The name of the configuration file. Where you put it is anyone's guess!"""


def load_config():
    config = ConfigParser()
    for loc in POSSIBLE_CONFIG_LOCATIONS:
        try: 
            with (loc / CONFIG_FILE_NAME).open("r") as source:
                config.read_file(source)
        except IOError:
            pass
        else:
            print(f"Found config at {loc}")
            # print(config["KAFKA"]["topic"])

    return config


CONFIG = load_config()


SPEEDTEST_CMD = CONFIG["DEFAULT"]["speedtest_command"]


BACKUP_FILE_PATH = Path(CONFIG["BACKUP"]["file_path"])
"""The file path used to store the results locally."""


def send_to_kafka(results: str):
    kafka_conf = CONFIG["KAFKA"]
    conf = {
        "bootstrap.brokers": kafka_conf["bootstrap.brokers"],
        "client.id": kafka_conf["client.id"]
    }
    producer = Producer(conf)
    try:
        producer.produce(kafka_conf["topic"], key=kafka_conf["key"], value=results)
    except ProduceError as e:
        print(f"Error sending data to Kafka: {e.kafka_message}")
        write_results(results, to_path=Path(CONFIG["KAFKA"]["file_path"]))
        # TODO - another script will periodically check this file and retry any entries
    else:
        producer.flush()


def write_results(results: str, to_path=None):
    if not to_path:
        to_path = BACKUP_FILE_PATH

    if not to_path.parent.exists():
        to_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Saving data to: {to_path}")
    with to_path.open("a") as fp:
        fp.write(results)


def call_speedtest():
    cmd_params = [SPEEDTEST_CMD, "--json", "--secure"]
    resp = subprocess.run(cmd_params, capture_output=True, encoding=sys.getdefaultencoding())

    if resp.stderr:
        print(resp.stderr)

    return resp.stdout


def run():
    resp = call_speedtest()

    try:
        results = json.loads(resp)
        results["machine_name"] = CONFIG["KAFKA"]["key"]
    except json.JSONDecodeError:
        print("Unable to parse results")
        results = resp
    else:
        print(results)
        results = json.dumps(results)

    if CONFIG["KAFKA"].getboolean("enabled"):
        send_to_kafka(resp)
    if CONFIG["BACKUP"].getboolean("enabled"):
        write_results(resp)


if __name__ == "__main__":
    run()
