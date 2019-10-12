from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from common_util import bash_operator_for_spark_submit

base_package = "org.pycon.za"


def master_tables_subdag(
    parent_dag_name, child_dag_name, args, retail_id, schema_name, env_type
):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval=None,
        concurrency=20,
    )

    sleep_task = BashOperator(
        task_id="sleep_for_3", bash_command="sleep 3", dag=dag_subdag
    )
    sleep_task.set_downstream(
        [
            bash_operator_for_spark_submit(
                f"{base_package}.master-tables",
                "Item-master",
                dag_subdag,
                retail_id,
                schema_name,
                env_type,
            ),
            bash_operator_for_spark_submit(
                f"{base_package}.master-tables",
                "Outlets-master",
                dag_subdag,
                retail_id,
                schema_name,
                env_type,
            ),
            bash_operator_for_spark_submit(
                f"{base_package}.master-tables",
                "Transactions",
                dag_subdag,
                retail_id,
                schema_name,
                env_type,
            ),
            bash_operator_for_spark_submit(
                f"{base_package}.master-tables",
                "Manufacturer",
                dag_subdag,
                retail_id,
                schema_name,
                env_type,
            ),
        ]
    )
    return dag_subdag
