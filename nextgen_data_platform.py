import os
import re
from datetime import datetime
from subprocess import Popen, PIPE, STDOUT

from airflow.executors import LocalExecutor
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable
from common_util import (
    get_config_values,
    conn_establish,
    insert_query,
    get_resultset_from_query,
    bash_operator_for_rdbms,
    bash_operator_for_spark_submit,
    execute_unix_command,
)
from nextgen_data_master_tables_subdag import master_tables_subdag

DAG_NAME = "nextgen_data_platform"
BASE_PACKAGE = "org.pycon.za"
RETAIL_ID = Variable.get("retail_id")
ENV_TYPE = Variable.get("env_type")

config_values = get_config_values(ENV_TYPE)

args = {"owner": "airflow", "start_date": datetime(2017, 3, 20)}

dag = DAG(dag_id=DAG_NAME, default_args=args, schedule_interval=None, concurrency=8)


def decipher(**kwargs):
    encrypted_username = config_values["username"]
    encrypted_password = config_values["password"]
    encrypt_key = config_values["passphrase"]
    application_jar = (
        config_values["airflow_dags_path"] + "dags/cryptography-decrypt-only.jar"
    )
    class_name = "org.pycon.za.cryptography.CryptographicCore"
    process = Popen(
        [
            "java",
            "-cp",
            application_jar,
            class_name,
            encrypted_username,
            encrypted_password,
            encrypt_key,
        ],
        stdout=PIPE,
        stderr=STDOUT,
    )

    decrypted_cred = None
    for line in process.stdout:
        decrypted_cred = line.rstrip().split(",")
    return decrypted_cred


def get_instruments(**kwargs):
    server_name = config_values["database_server"]
    decrypted_username = decipher(**kwargs)[0]
    decrypted_password = decipher(**kwargs)[1]
    schema_name = get_resultset_from_query(
        RETAIL_ID,
        server_name,
        decrypted_username,
        decrypted_password,
        "select schema_name from nextgen.dbo.retailer where retail_id=",
    )
    return schema_name


push_instruments = PythonOperator(
    task_id="push_instruments",
    python_callable=get_instruments,
    provide_context=True,
    dag=dag,
)

schema_name = '{{ ti.xcom_pull(task_ids = "push_instruments" , dag_id = "nextgen_data_platform")}}'


def create_server_details(**kwargs):
    decrypted_cred = get_instruments(**kwargs)
    server_name = config_values["database_server"]
    decrypted_cred.append(server_name)
    server_conn_part = (
        decrypted_cred[2] + " -U " + decrypted_cred[0] + " -P " + decrypted_cred[1]
    )
    return server_conn_part


push_server_details = PythonOperator(
    task_id="push_server_details",
    python_callable=create_server_details,
    provide_context=True,
    dag=dag,
)

server_details = '{{ ti.xcom_pull(task_ids="create_table_structure" , dag_id = "nextgen_data_platform")}}'

create_table_structure = bash_operator_for_rdbms(
    "create_table_structure",
    server_details,
    RETAIL_ID,
    schema_name,
    config_values["airflow_dags_path"]
    + "nextgen_data_platform_create_table_structure.sql",
    dag,
    0,
)
create_constraint_task = bash_operator_for_rdbms(
    "create_constraint_task",
    server_details,
    RETAIL_ID,
    schema_name,
    config_values["airflow_dags_path"]
    + "nextgen_data_platform_constraint_creation.sql",
    dag,
    0,
)


def check_history_run(**kwargs):
    hdfs_command = [
        "hdfs",
        "dfs",
        "-test",
        "-e",
        config_values["hdfc_history_flag_path"],
    ]
    ret, out, err = execute_unix_command(hdfs_command)
    if ret:
        return "start_historical_load"
    else:
        return "start_incremental_load"


branch_task = BranchPythonOperator(
    task_id="branch_task",
    python_callable=check_history_run,
    trigger_rule="one_success",
    provide_context=True,
    run_as_user="spark",
    dag=dag,
)

history_load_done = BashOperator(
    task_id="history_load_check_task", bash_command="sleep 2", dag=dag
)
data_load_done = BashOperator(task_id="data_load_done", bash_command="sleep 2", dag=dag)

unix_chmod_command = f"chmod 777 {config_values['airflow_dags_path']}/sql/*.sql"
unix_chmod_task = BashOperator(
    task_id="chmod_command", bash_command=unix_chmod_command, dag=dag
)

master_tables_load = SubDagOperator(
    executor=LocalExecutor(),
    task_id="master_tables_load",
    subdag=master_tables_subdag(
        DAG_NAME, "master_tables_load", args, RETAIL_ID, schema_name, ENV_TYPE
    ),
    default_args=args,
    dag=dag,
)

market_baskets_task = bash_operator_for_spark_submit(
    f"{BASE_PACKAGE}.transactional-tables",
    "MarketBasket",
    dag,
    RETAIL_ID,
    schema_name,
    ENV_TYPE,
)
transaction_line_item_task = bash_operator_for_spark_submit(
    f"{BASE_PACKAGE}.transactional-tables",
    "TransactionLineItem",
    dag,
    RETAIL_ID,
    schema_name,
    ENV_TYPE,
)
outlets_by_date_task = bash_operator_for_spark_submit(
    f"{BASE_PACKAGE}.transactional-tables",
    "OutletsByDate",
    dag,
    RETAIL_ID,
    schema_name,
    ENV_TYPE,
)
items_by_date_task = bash_operator_for_spark_submit(
    f"{BASE_PACKAGE}.transactional-tables",
    "ItemsByDate",
    dag,
    RETAIL_ID,
    schema_name,
    ENV_TYPE,
)

push_instruments.set_downstream(push_server_details)
branch_task.set_upstream(push_server_details)
branch_task.set_downstream(master_tables_load)

branch_task.set_downstream(history_load_done)
master_tables_load.set_downstream(create_table_structure)
history_load_done.set_downstream(create_table_structure)
create_table_structure.set_downstream(unix_chmod_task)
unix_chmod_task.set_downstream(market_baskets_task)
market_baskets_task.set_downstream(
    [transaction_line_item_task, outlets_by_date_task, items_by_date_task]
)
data_load_done.set_upstream(
    [transaction_line_item_task, outlets_by_date_task, items_by_date_task]
)
create_constraint_task.set_upstream(data_load_done)
