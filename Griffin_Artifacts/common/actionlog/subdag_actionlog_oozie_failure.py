from airflow import models

from uds_dag.common.oozie2pinwheel.oozie2pinwheel_libs.operator.emr_submit_and_monitor_step_operator import \
    EmrSubmitAndMonitorStepOperator

#def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval,cluster_id,aws_conn_id,check_interval,JOB_PROPS):
    with models.DAG(
        "{0}.{1}".format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,  # Change to suit your needs
        start_date=start_date,  # Change to suit your needs
    ) as dag:

        run_failure_oozie = EmrSubmitAndMonitorStepOperator(
            task_id="run_failure_oozie",
             steps=[
                {
                    "Name": "run_failure_oozie_" + parent_dag_name,
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "hive-script",
                            "--run-hive-script",
                            "--args",
                            "-f",
                            JOB_PROPS['actionlog_script'],
                            "-d",
                            "database_name="+JOB_PROPS['actionlog_database_name'],
                            "-d",
                            "action_reason=oozie failure",
                            "-d",
                            "action_data=NULL",
                            "-d",
                            "bundle_name="+JOB_PROPS['actionlog_bundle_name'],
                            "-d",
                            "coordinator_name="+JOB_PROPS['actionlog_coordinator_name'],
                            "-d",
                            "workflow_name="+JOB_PROPS['actionlog_workflow_name'],
                            "-d",
                            "workflow_id="+parent_dag_name,
                            "-d",
                            "dag_name="+parent_dag_name,
                            "-d",
                            "table_name="+JOB_PROPS['actionlog_table_name'],
                        ],
                    },
                }
            ],
            job_flow_id=cluster_id,
            aws_conn_id=aws_conn_id,
            check_interval=int(check_interval),
            job_name="run_failure_oozie",
        )

    return dag
