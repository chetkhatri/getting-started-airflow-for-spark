import configparser
import os
import sys

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator

APPLICATION_JAR = "nextgen-data-platform-retail.jar"


def get_config_values(env_type):
    config = configparser.RawConfigParser()
    config.read(
        [
            f"/home/chetankhatri/airflow_home/dags/config.txt",
            f"/home/chetankhatri/airflow_home/dags/spark_config.txt",
        ]
    )

    DATABASE_SERVER = config.get(env_type, "database_server")
    USERNAME = config.get(env_type, "username")
    PASSWORD = config.get(env_type, "password")
    PASSPHRASE = config.get(env_type, "passphrase")
    JOBS_LOCATION = config.get("SPARK_CONFIG", "jobs_location")
    DATA_VALIDATION_CHECK = config.get("SPARK_CONFIG", "data_validation_check")
    SPARK_SUBMIT_COMMAND = config.get("SPARK_CONFIG", "spark_submit_command")
    SCHEMA_NAME = config.get("DATABASE_CONFIG", "schema_name")
    CHANGE_TRACK = config.get("DATABASE_CONFIG", "is_change_track")
    AIRFLOW_DAGS_PATH = config.get("SPARK_CONFIG", "airflow_dags_path")
    SPARK_HOME = config.get("SPARK_CONFIG", "spark_home")
    SPARK_BIN = config.get("SPARK_CONFIG", "spark_bin")
    HDFC_HISTORY_FLAG_PATH = config.get("SPARK_CONFIG", "hdfc_history_flag_path")
    SQLCMD_PATH = config.get("SPARK_CONFIG", "sqlcmd_path")
    config_dict = {
        "database_server": DATABASE_SERVER,
        "username": USERNAME,
        "password": PASSWORD,
        "passphrase": PASSPHRASE,
        "jobs_location": JOBS_LOCATION,
        "data_validation_check": DATA_VALIDATION_CHECK,
        "spark_submit_command": SPARK_SUBMIT_COMMAND,
        "schema_name": SCHEMA_NAME,
        "change_track": CHANGE_TRACK,
        "airflow_dags_path": AIRFLOW_DAGS_PATH,
        "spark_home": SPARK_HOME,
        "spark_bin": SPARK_BIN,
        "hdfc_history_flag_path": HDFC_HISTORY_FLAG_PATH,
        "sqlcmd_path": SQLCMD_PATH,
    }

    return config_dict


def conn_establish(database_server, username, password):
    import pypyodbc as pyodbc

    connection = pyodbc.connect(
        f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={database_server};UID={username};PWD={password};TrustServerCertificate=yes;Connection Timeout=60"
    )
    return connection


def get_resultset_from_query(retailer_id, database_server, username, password, query):
    conn = conn_establish(database_server, username, password)
    cursor = conn.cursor()
    cursor.execute(f"{query}{retailer_id}")
    row = cursor.fetchone()
    try:
        if row == None:
            SystemExit
            return ""
        elif row[2] == "V2":
            return row[1]
        else:
            return row[1]
    finally:
        cursor.close()
        conn.close()


def insert_query(database_server, username, password, query):
    conn = conn_establish(database_server, username, password)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


def bash_operator_for_rdbms(
    task_id, server_details, retail_id, schema_name, file_name, dag, is_change_tracking
):
    operator = BashOperator(
        task_id=task_id,
        bash_command=f"export retail_id={retail_id};export change_tracking_flag={is_change_tracking};sqlcmd -S ${server_details} -d {schema_name} -i {file_name}",
        dag=dag,
    )
    return operator


def bash_operator_for_spark_submit(
    class_path,
    spark_class_name,
    dag,
    retail_id,
    schema_name,
    env_type,
    trigger_rule="all_success",
):
    operator = BashOperator(
        task_id=spark_class_name,
        bash_command=" {{ params.spark_submit }} "
        + " --conf spark.dynamicAllocation.enabled="
        + '{{ var.json.dag_config[dag_run.conf["size_is"]].'
        + spark_class_name
        + ".dynamicAllocation }}"
        + " --conf spark.dynamicAllocation.minExecutors="
        + '{{ var.json.dag_config[dag_run.conf["size_is"]].'
        + spark_class_name
        + ".minExecutors }}"
        + " --conf spark.dynamicAllocation.initialExecutors="
        + '{{ var.json.dag_config[dag_run.conf["size_is"]].'
        + spark_class_name
        + ".minExecutors }}"
        + " --conf spark.dynamicAllocation.maxExecutors="
        + '{{ var.json.dag_config[dag_run.conf["size_is"]].'
        + spark_class_name
        + ".maxExecutors }}"
        + " --executor-cores "
        + '{{ var.json.dag_config[dag_run.conf["size_is"]].'
        + spark_class_name
        + ".executor_cores }}"
        + " --executor-memory "
        + '{{ var.json.dag_config[dag_run.conf["size_is"]].'
        + spark_class_name
        + ".executor_memory }}"
        + " --driver-memory "
        + '{{ var.json.dag_config[dag_run.conf["size_is"]].'
        + spark_class_name
        + ".driver_memory }}"
        + " --name airflow-"
        + str(retail_id)
        + "-{{params.job_name}} --class {{ params.class_name }}  {{params.jar_name}} "
        + env_type
        + " "
        + str(retail_id)
        + " "
        + schema_name,
        params={
            "spark_submit": get_config_values(env_type)["spark_submit_command"],
            "class_name": class_path + spark_class_name,
            "job_name": spark_class_name,
            "spark_class_name": spark_class_name,
            "retail_id": retail_id,
            "jar_name": APPLICATION_JAR,
        },
        run_as_user="spark",
        dag=dag,
        trigger_rule=trigger_rule,
    )
    return operator


def execute_unix_command(args_list):
    import subprocess

    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = proc.communicate()
    response_code = proc.returncode
    return response_code, output, error
