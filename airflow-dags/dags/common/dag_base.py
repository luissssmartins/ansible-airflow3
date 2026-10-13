from abc import ABC, abstractmethod
from datetime import timedelta

import pendulum
from airflow.sdk import DAG

class DAGBase(ABC):
    def __init__(
        self,
        dag_id,
        description=None,
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
        catchup=False,
        max_active_runs=1,
        owner="airflow",
        retries=1,
        retry_delay=timedelta(minutes=5),
        tags=(),
    ):
        self.dag_id = dag_id
        self.description = description
        self.schedule = schedule
        self.start_date = start_date
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        self.owner = owner
        self.retries = retries
        self.retry_delay = retry_delay
        self.tags = tags

    def default_args(self):
        return {
            "owner": self.owner,
            "retries": self.retries,
            "retry_delay": self.retry_delay,
        }

    @abstractmethod
    def build(self): ...

    def create(self):
        with DAG(
            dag_id=self.dag_id,
            description=self.description,
            schedule=self.schedule,
            start_date=self.start_date,
            catchup=self.catchup,
            max_active_runs=self.max_active_runs,
            default_args=self.default_args(),
            tags=list(self.tags),
        ) as dag:
            self.build()
        return dag
