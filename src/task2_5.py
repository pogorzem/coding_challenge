from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# default args
args = {"owner": "airflow", "start_date": days_ago(1)}

# setup dag properties
dag = DAG(
    dag_id="task_2_5",
    default_args=args,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None,
)

# generate dag and task dependencies
with dag:

    """
    This is easiest solution but as I'm using airflow 2.0 I did go with group solution which look a lot clear to me
    and is easier to understand on diagram and extend, please check README screenshots for comparison

    t1 = DummyOperator(task_id="Task1")
    t2 = DummyOperator(task_id="Task2")
    t3 = DummyOperator(task_id="Task3")
    t4 = DummyOperator(task_id="Task4")
    t5 = DummyOperator(task_id="Task5")
    t6 = DummyOperator(task_id="Task6")

    t1 >> [t2, t3]
    t2 >> [t4, t5, t6]
    t3 >> [t4, t5, t6]
    """

    with TaskGroup(group_id="Group1") as group1:
        t1 = DummyOperator(task_id="Task1")

    with TaskGroup(group_id="Group2") as group2:
        t2 = DummyOperator(task_id="Task2")

        t3 = DummyOperator(task_id="Task3")

    with TaskGroup(group_id="Group3") as group3:
        t4 = DummyOperator(task_id="Task4")

        t5 = DummyOperator(task_id="Task5")

        t6 = DummyOperator(task_id="Task6")

    group1 >> group2 >> group3
