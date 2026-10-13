import logging
import psycopg2
import json
import traceback
import time
import paramiko

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ParabellumInfra:
  def __init__(self, *args, **kwargs):
    self.debug = kwargs.get("debug", False)
    self.only_metrics = kwargs.get("only_metrics", False)
    self.only_info = kwargs.get("only_info", False)

    self.host = kwargs.get("host", None)
    self.database = kwargs.get("database", None)
    self.username = kwargs.get("username", None)
    self.password = kwargs.get("password", None)
    self.port = kwargs.get("port", 5432)

  def __get_cursor__(self):
    try:
      connection = psycopg2.connect(
        host=self.host,
        database=self.database,
        user=self.username,
        password=self.password,
        port=self.port
      )

      cursor = connection.cursor()

      return cursor, connection
    except:
      traceback.print_exc()

  def __collect_pg_stats__(self):
      cursor = self.__get_cursor__()[0]

      cursor.execute("""
          select
              version() AS version,
              now() - pg_postmaster_start_time() AS uptime,
              count(*) AS total,
              count(*) FILTER (WHERE state = 'active') AS active,
              count(*) FILTER (WHERE state = 'idle') AS idle,
              count(*) FILTER (WHERE wait_event_type IS NOT NULL AND state = 'active') AS waiting
          from pg_stat_activity;
      """)
      stats = cursor.fetchone()

      cursor.execute("""
          select
              coalesce(query, '') as query,
              coalesce(now() - query_start, '0'::interval) as duration
          from pg_stat_activity
          where state = 'active'
            and query not ilike '%pg_stat_activity%'
          order by duration desc
          limit 1;
      """)
      longest = cursor.fetchone()

      return {
          'postgresql_version': stats['version'] if stats else '',
          'postgresql_uptime': stats['uptime'] if stats else '0',
          'postgresql_connections': stats['total'] if stats else 0,
          'postgresql_active_connections': stats['active'] if stats else 0,
          'postgresql_idle_connections': stats['idle'] if stats else 0,
          'postgresql_waiting_connections': stats['waiting'] if stats else 0,
          'postgresql_longest_query': longest['query'] if longest else '',
          'postgresql_longest_query_duration': longest['duration'] if longest else '0',
      }
  
class SSHPing:
  def __init__(self, *args, **kwargs):
    self.debug = kwargs.get("debug", False)

    self.host = kwargs.get("host", None)
    self.port = kwargs.get("port", 22)
    self.username = kwargs.get("username", "root")
    self.password = kwargs.get("password", None)
    self.key_filename = kwargs.get("key_filename", "/root/.ssh/id_rsa")
    self.timeout = kwargs.get("timeout", 10)

  def ping(self, host=None):
    host = host or self.host
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    start = time.monotonic()
    try:
      client.connect(
        hostname=host,
        port=self.port,
        username=self.username,
        password=self.password,
        key_filename=self.key_filename,
        timeout=self.timeout,
        banner_timeout=self.timeout,
        auth_timeout=self.timeout
      )
      result = {"host": host, "port": self.port, "status": True, "latency_ms": round((time.monotonic() - start) * 1000, 2), "error": None}
    except Exception as error:
      result = {"host": host, "port": self.port, "status": False, "latency_ms": None, "error": str(error)}
    finally:
      client.close()

    if self.debug:
      log.info("SSH Ping: %s" %(json.dumps(result, indent=4, default=str)))
    return result

  def ping_all(self, hosts):
    return [self.ping(host) for host in hosts]

class SSHShell:
  def __init__(self, *args, **kwargs):
    self.debug = kwargs.get("debug", False)

    self.host = kwargs.get("host", None)
    self.port = kwargs.get("port", 22)
    self.username = kwargs.get("username", "root")
    self.password = kwargs.get("password", None)
    self.key_filename = kwargs.get("key_filename", "/root/.ssh/id_rsa")
    self.timeout = kwargs.get("timeout", 10)

    self.client = None

  def __enter__(self):
    self.__get_client__()
    return self

  def __exit__(self, *args):
    self.close()

  def __get_client__(self):
    if self.client is None:
      self.client = paramiko.SSHClient()
      self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      self.client.connect(
        hostname=self.host,
        port=self.port,
        username=self.username,
        password=self.password,
        key_filename=self.key_filename,
        timeout=self.timeout,
        banner_timeout=self.timeout,
        auth_timeout=self.timeout
      )
    return self.client

  def execute(self, command, timeout=None):
    try:
      stdin, stdout, stderr = self.__get_client__().exec_command(command, timeout=timeout or self.timeout)
      output = stdout.read().decode(errors="replace").strip()
      error = stderr.read().decode(errors="replace").strip()
      result = {"command": command, "exit_code": stdout.channel.recv_exit_status(), "stdout": output, "stderr": error}

      if self.debug:
        log.info("SSH shell: %s" %(json.dumps(result, indent=4, default=str)))
      return result
    except:
      traceback.print_exc()

  def execute_many(self, commands, stop_on_error=False):
    results = []
    for command in commands:
      result = self.execute(command)
      results.append(result)
      if stop_on_error and (result is None or result["exit_code"] != 0):
        break
    return results

  def close(self):
    if self.client is not None:
      self.client.close()
      self.client = None
