import json
import logging
import socket
import struct
import time

from airflow.configuration import conf
from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


class ZabbixSender:
    def __init__(self):
        self.server = conf.get("zabbix", "server", fallback="")
        self.port = conf.getint("zabbix", "port", fallback=10051)
        self.host = conf.get("zabbix", "host", fallback=socket.gethostname())
        self.timeout = conf.getint("zabbix", "timeout", fallback=5)

    def send(self, values):
        if not self.server:
            return
        clock = int(time.time())
        data = [{"host": self.host, "key": key, "value": str(value), "clock": clock} for key, value in values]
        payload = json.dumps({"request": "sender data", "data": data}).encode()
        packet = b"ZBXD\x01" + struct.pack("<II", len(payload), 0) + payload
        try:
            with socket.create_connection((self.server, self.port), timeout=self.timeout) as connection:
                connection.sendall(packet)
                header = connection.recv(13)
                length = struct.unpack("<I", header[5:9])[0]
                response = b""
                while len(response) < length:
                    response += connection.recv(length - len(response))
            log.info("Zabbix: %s", json.loads(response).get("info"))
        except Exception:
            log.exception("Falha ao enviar dados ao Zabbix")


class ZabbixListener:
    def __init__(self):
        self.sender = ZabbixSender()

    def discovery(self, dag_id):
        return "airflow.dag.discovery", json.dumps([{"{#DAG_ID}": dag_id}])

    @hookimpl
    def on_dag_run_running(self, dag_run, msg):
        self.sender.send([self.discovery(dag_run.dag_id)])

    @hookimpl
    def on_dag_run_success(self, dag_run, msg):
        self.finish(dag_run, 1)

    @hookimpl
    def on_dag_run_failed(self, dag_run, msg):
        self.finish(dag_run, 0)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, error):
        self.sender.send([
            (f"airflow.task.failed[{task_instance.dag_id}]", f"{task_instance.task_id}: {error}"),
        ])

    def finish(self, dag_run, status):
        duration = (dag_run.end_date - dag_run.start_date).total_seconds() if dag_run.end_date and dag_run.start_date else 0
        self.sender.send([
            self.discovery(dag_run.dag_id),
            (f"airflow.dagrun.status[{dag_run.dag_id}]", status),
            (f"airflow.dagrun.duration[{dag_run.dag_id}]", duration),
        ])


class ZabbixPlugin(AirflowPlugin):
    name = "zabbix"
    listeners = [ZabbixListener()]
