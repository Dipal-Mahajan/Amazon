import uds_dag.common.actionlog.subdag_actionlog_oozie_begin as subdag_actionlog_oozie_begin
import uds_dag.common.actionlog.subdag_actionlog_oozie_failure as subdag_actionlog_oozie_failure
import uds_dag.common.actionlog.subdag_actionlog_oozie_success as subdag_actionlog_oozie_success

from airflow import models
from airflow.operators import dummy_operator, email_operator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils import dates

from uds_dag.common.oozie2pinwheel.oozie2pinwheel_libs import functions
from uds_dag.common.oozie2pinwheel.oozie2pinwheel_libs.operator.emr_submit_and_monitor_step_operator import \
    EmrSubmitAndMonitorStepOperator
from uds_dag.common.oozie2pinwheel.oozie2pinwheel_libs.property_utils import PropertySet

from airflow.exceptions import AirflowException

from datetime import datetime,timedelta
from airflow.contrib.hooks.aws_hook import AwsHook
from uds_dag.common.emr_launch_lib_griffin import CreateEMROperator, CreateEMRSensor, TerminateEMROperator

import os

DAG_ID=os.path.basename(__file__).replace(".py", "")

BUNDLE_NAME='ref_fraud_order'

emr_sc_tags = [
    {
        'Key': 'dataPipeline',
        'Value': 'ref_fraud_order'
    },
    {
        'Key': 'teamName',
        'Value': 'EDT'
    },
    {
        'Key': 'organization',
        'Value': 'D-SPA'
    },
    {
        'Key': 'onCallGroup',
        'Value': 'DEV-EDT-OnCall'
    },
    {
        'Key': 'teamSlackChannel',
        'Value': 'edt-airflow-alerts'
    },
    {
        'Key': 'managedByMWAA',
        'Value': 'true'
    },
    {
        'Key': 'doNotShutDown',
        'Value': 'true'
    },
]

CONFIG = {
    "aws_conn_id": "edt_aws_conn",
    "aws_region": "us-west-2",
    "check_interval": "30"
}

ssm_client = AwsHook(aws_conn_id=CONFIG["aws_conn_id"]).get_client_type('ssm')
env_param = ssm_client.get_parameter(Name="/AdminParams/Team/Environment")
env_param_val = env_param["Parameter"]["Value"]

s3_bucket_name_for_script="s3://gd-ckpetlbatch-"+ env_param_val+ "-code"
s3_bucket_name_for_flag="gd-ckpetlbatch-"+ env_param_val + "-tmp"

env_code = s3_bucket_name_for_script

if env_param_val == "dev-private":
    env_data = "s3://gd-ckpetlbatch-" + env_param_val + "-data"
else :
    env_data = "s3://gd-ckpetlbatch-" + env_param_val + "-hadoop-migrated"


emr_sc_bootstrap_file_path = s3_bucket_name_for_script + "/util/" + BUNDLE_NAME + "/bootstrap_griffin.sh"

JOB_PROPS = {
    "accuracy_sh" :  s3_bucket_name_for_script + '/uds-griffin-validation/' + BUNDLE_NAME + '/accuracy.sh',
    "total_count_sh" : s3_bucket_name_for_script + '/uds-griffin-validation/' + BUNDLE_NAME + '/total_count.sh'
    #"unique_records_sh" : s3_bucket_name_for_script + '/uds-griffin-validation/' + BUNDLE_NAME + '/unique_records.sh',
    #"distinct_records_sh" : s3_bucket_name_for_script + '/uds-griffin-validation/' + BUNDLE_NAME + '/distinct_records.sh',
    #"duplicate_records_sh" : s3_bucket_name_for_script + '/uds-griffin-validation/' + BUNDLE_NAME +  '/duplicate_records.sh',
    #"incomplete_records_sh" : s3_bucket_name_for_script + '/uds-griffin-validation/' + BUNDLE_NAME + '/incomplete_records.sh',
    #"profile_records_sh" : s3_bucket_name_for_script + '/uds-griffin-validation/' + BUNDLE_NAME + '/profile_records.sh'
}

TEMPLATE_ENV = {**CONFIG, **JOB_PROPS, "functions": functions}

with models.DAG(
    DAG_ID,
    schedule_interval='30 21 * * *',  # Change to suit your needs
    start_date=datetime(2021, 4, 5),  # Change to suit your needs
    user_defined_macros=TEMPLATE_ENV,
    concurrency=15,
    max_active_runs=1,
    tags=['griffin_validation_dag']
) as dag:


    create_emr_cluster = CreateEMROperator(
        task_id='create_emr_cluster',
        conn_id=CONFIG["aws_conn_id"],
        emr_cluster_name='mwaa_'+ DAG_ID + "_"+ "{{ ts_nodash }}",
        master_instance_type='m5.8xlarge',
        core_instance_type='r5d.8xlarge',
        num_core_nodes='3',
        emr_release_label='emr-5.32.0',
        emr_sc_provisioning_artifact_name='1.5.5',
        emr_sc_bootstrap_file_path=emr_sc_bootstrap_file_path,
        emr_step_concurrency='2',
        emr_sc_tags=emr_sc_tags
    )

    create_emr_cluster_error = dummy_operator.DummyOperator(
        task_id="create_emr_cluster_error", trigger_rule="one_failed"
    )
    create_emr_cluster_ok = dummy_operator.DummyOperator(
        task_id="create_emr_cluster_ok", trigger_rule="one_success"
    )
    create_emr_cluster.set_downstream(create_emr_cluster_error)
    create_emr_cluster.set_downstream(create_emr_cluster_ok)

    chk_create_emr_cluster = CreateEMRSensor(
        task_id='chk_create_emr_cluster',
        conn_id=CONFIG["aws_conn_id"],
        provisioned_record_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value')[0] }}",
        dag=dag,
    )

    chk_create_emr_cluster_error = dummy_operator.DummyOperator(
        task_id="chk_create_emr_cluster_error", trigger_rule="one_failed"
    )
    chk_create_emr_cluster_ok = dummy_operator.DummyOperator(
        task_id="chk_create_emr_cluster_ok", trigger_rule="one_success"
    )
    chk_create_emr_cluster.set_downstream(chk_create_emr_cluster_error)
    chk_create_emr_cluster.set_downstream(chk_create_emr_cluster_ok)

    create_emr_cluster_ok.set_downstream(chk_create_emr_cluster)

    forking = dummy_operator.DummyOperator(task_id="forking", trigger_rule="one_success")
    chk_create_emr_cluster_ok.set_downstream(forking)

    run_accuracy_sh = EmrSubmitAndMonitorStepOperator(
        task_id="run_accuracy_sh",
        steps=[
            {
                "Name": "run_accuracy_sh",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
                    "Args": [
                        "{{accuracy_sh}}"
                    ],
                },
            }
        ],
        job_flow_id="{{ task_instance.xcom_pull(task_ids='chk_create_emr_cluster', key='job_flow_id') }}",
        aws_conn_id=CONFIG["aws_conn_id"],
        check_interval=int(CONFIG["check_interval"]),
        job_name="run_accuracy_sh",
    )

    run_total_count_sh = EmrSubmitAndMonitorStepOperator(
        task_id="run_total_count_sh",
        steps=[
            {
                "Name": "run_total_count_sh",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
                    "Args": [
                        "{{total_count_sh}}"
                    ],
                },
            }
        ],
        job_flow_id="{{ task_instance.xcom_pull(task_ids='chk_create_emr_cluster', key='job_flow_id') }}",
        aws_conn_id=CONFIG["aws_conn_id"],
        check_interval=int(CONFIG["check_interval"]),
        job_name="run_total_count_sh",
    )
    """
    run_unique_records_sh  = EmrSubmitAndMonitorStepOperator(
        task_id="run_unique_records_sh",
        steps=[
            {
                "Name": "run_unique_records_sh",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
                    "Args": [
                        "{{unique_records_sh}}"
                    ],
                },
            }
        ],
        job_flow_id="{{ task_instance.xcom_pull(task_ids='chk_create_emr_cluster', key='job_flow_id') }}",
        aws_conn_id=CONFIG["aws_conn_id"],
        check_interval=int(CONFIG["check_interval"]),
        job_name="run_unique_records_sh",
    )

    run_distinct_records_sh = EmrSubmitAndMonitorStepOperator(
        task_id="run_distinct_records_sh",
        steps=[
            {
                "Name": "run_distinct_records_sh",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
                    "Args": [
                        "{{distinct_records_sh}}"
                    ],
                },
            }
        ],
        job_flow_id="{{ task_instance.xcom_pull(task_ids='chk_create_emr_cluster', key='job_flow_id') }}",
        aws_conn_id=CONFIG["aws_conn_id"],
        check_interval=int(CONFIG["check_interval"]),
        job_name="run_distinct_records_sh",
    )

    run_duplicate_records_sh = EmrSubmitAndMonitorStepOperator(
        task_id="run_duplicate_records_sh",
        steps=[
            {
                "Name": "run_duplicate_records_sh",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
                    "Args": [
                        "{{duplicate_records_sh}}"
                    ],
                },
            }
        ],
        job_flow_id="{{ task_instance.xcom_pull(task_ids='chk_create_emr_cluster', key='job_flow_id') }}",
        aws_conn_id=CONFIG["aws_conn_id"],
        check_interval=int(CONFIG["check_interval"]),
        job_name="run_duplicate_records_sh",
    )

    run_incomplete_records_sh = EmrSubmitAndMonitorStepOperator(
        task_id="run_incomplete_records_sh",
        steps=[
            {
                "Name": "run_incomplete_records_sh",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
                    "Args": [
                        "{{incomplete_records_sh}}"
                    ],
                },
            }
        ],
        job_flow_id="{{ task_instance.xcom_pull(task_ids='chk_create_emr_cluster', key='job_flow_id') }}",
        aws_conn_id=CONFIG["aws_conn_id"],
        check_interval=int(CONFIG["check_interval"]),
        job_name="run_incomplete_records_sh",
    )

    run_profile_records_sh = EmrSubmitAndMonitorStepOperator(
        task_id="run_profile_records_sh",
        steps=[
            {
                "Name": "run_profile_records_sh",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
                    "Args": [
                        "{{profile_records_sh}}"
                    ],
                },
            }
        ],
        job_flow_id="{{ task_instance.xcom_pull(task_ids='chk_create_emr_cluster', key='job_flow_id') }}",
        aws_conn_id=CONFIG["aws_conn_id"],
        check_interval=int(CONFIG["check_interval"]),
        job_name="run_profile_records_sh",
    )
    """
    joining = dummy_operator.DummyOperator(task_id="joining", trigger_rule="all_success")

    terminate_emr_cluster = TerminateEMROperator(
        task_id='terminate_emr_cluster',
        conn_id=CONFIG["aws_conn_id"],
        provisioned_product_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value')[1] }}",
        dag=dag,
    )

    forking.set_downstream(run_accuracy_sh)
    forking.set_downstream(run_total_count_sh)
    #forking.set_downstream(run_unique_records_sh)
    #forking.set_downstream(run_distinct_records_sh)
    #forking.set_downstream(run_duplicate_records_sh)
    #forking.set_downstream(run_incomplete_records_sh)
    #forking.set_downstream(run_profile_records_sh)

    run_accuracy_sh.set_downstream(joining)
    run_total_count_sh.set_downstream(joining)
    #run_unique_records_sh.set_downstream(joining)
    #run_distinct_records_sh.set_downstream(joining)
    #run_duplicate_records_sh.set_downstream(joining)
    #run_incomplete_records_sh.set_downstream(joining)
    #run_profile_records_sh.set_downstream(joining)

    joining.set_downstream(terminate_emr_cluster)
