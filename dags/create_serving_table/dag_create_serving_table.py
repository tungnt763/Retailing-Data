from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from lib.utils import read_metadata
import os

default_args = {
    'owner': 'tungnt763',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

metadata = read_metadata()
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
SQL_TEMPLATE_PATH = os.path.join(DAG_FOLDER, "sql_template", "dedup_data.sql")

def get_table_pk_and_columns(table_def):
    pk = table_def.get("primary_key", [])
    pk_str = ", ".join(pk)
    columns = []
    for field, prop in table_def["fields"].items():
        typ = prop.get("type", "string")
        if typ == "float":
            columns.append(f"{field}::NUMERIC AS {field}")
        elif typ == "int":
            columns.append(f"{field}::INTEGER AS {field}")
        elif typ == "date":
            columns.append(f"{field}::DATE AS {field}")
        elif typ == "datetime":
            columns.append(f"{field}::TIMESTAMP AS {field}")
        else:
            columns.append(field)
    columns += ["created_at", "batch_id", "run_id", "task_id"]
    columns_str = ", ".join(columns)
    return pk_str, columns_str

def render_sql_template(template_path, table_name, pk, columns):
    with open(template_path, "r") as f:
        template = f.read()
    return template.format(table_name=table_name, pk=pk, columns=columns)

@dag(
    default_args=default_args,
    description='Build Serving Table with Deduplication (expand, auto-cast)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def dag_build_serving_table_expand():

    test = BashOperator(
        task_id="ls_sql_template",
        bash_command="ls -l /opt/airflow/dags/create_serving_table/sql_template/"
    )

    @task
    def get_dedup_configs():
        configs = []
        for tbl in metadata["tables"]:
            table_name = tbl["table_name"]
            pk, columns = get_table_pk_and_columns(tbl)
            dedup_sql = render_sql_template(SQL_TEMPLATE_PATH, table_name, pk, columns)
            configs.append({"table_name": table_name, "dedup_sql": dedup_sql})
        return configs

    dedup_configs = get_dedup_configs()

    dedup_tasks = PostgresOperator.partial(
        task_id="dedup_table",
        postgres_conn_id='postgres',
    ).expand(
        sql=dedup_configs.map(lambda x: x["dedup_sql"])
    )

    create_serving_table = PostgresOperator(
        task_id="create_serving_table",
        postgres_conn_id='postgres',
        sql="sql_template/create_serving.sql"
    )

    test >> dedup_tasks >> create_serving_table

dag = dag_build_serving_table_expand()
