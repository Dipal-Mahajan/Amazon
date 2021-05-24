
import subprocess
import ast
import boto3

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

# TODO: Update doc strings

class CreateEMROperator(BaseOperator):
    
    emr_sc_tags = [
        {
            'Key': 'doNotShutDown',
            'Value': 'true'
        },
    ]

    template_fields = ['emr_cluster_name', 'aws_environment', 'emr_release_label', 'emr_sc_bootstrap_file_path']
    template_ext = ('.json',)

    @apply_defaults
    def __init__(self,
                 *,
                 conn_id='aws_default',
                 aws_environment='dev-private',
                 emr_cluster_name='edtscemrdefault',
                 master_instance_type='m5.2xlarge',
                 core_instance_type='c5.2xlarge',
                 num_core_nodes='2',
                 emr_release_label='emr-5.32.0',
                 emr_sc_bootstrap_file_path='s3://test-bucket/init',
                 emr_sc_provisioning_artifact_name='1.5.3',
                 emr_custom_job_flow_role_name_suffix='emr-ec2-default-role',
                 emr_custom_service_role_name_suffix='emr-default-role',
                 emr_step_concurrency='1',
                 emr_sc_tags,
                 **kwargs,
                 ):
        super().__init__(**kwargs)
        # Most frequently used params
        self.conn_id = conn_id
        self.aws_environment = aws_environment
        self.emr_cluster_name = emr_cluster_name
        self.master_instance_type = master_instance_type
        self.core_instance_type = core_instance_type
        self.num_core_nodes = str(num_core_nodes)
        # Less frequently used
        self.emr_release_label = emr_release_label
        self.emr_sc_bootstrap_file_path = emr_sc_bootstrap_file_path
        self.emr_sc_provisioning_artifact_name = emr_sc_provisioning_artifact_name
        self.emr_custom_job_flow_role_name_suffix = emr_custom_job_flow_role_name_suffix
        self.emr_custom_service_role_name_suffix = emr_custom_service_role_name_suffix
        # Internally set
        self.emr_sc_product_id = None
        self.emr_sc_provisioning_artifact_id = None
        self.emr_sc_launch_path_id = None
        self.emr_sc_tags = emr_sc_tags
        #self.aws_hook = None
        self.master_security_group_id = None
        self.slave_security_group_id = None
        self.subnet_id = None
        self.emr_step_concurrency=emr_step_concurrency


    '''
    def get_aws_hook(self):
            aws_hook = AwsHook(aws_conn_id=self.conn_id)
        return aws_hook
    '''
    def set_emr_service_product_ids(self):
        sc = AwsHook(aws_conn_id=self.conn_id).get_client_type('servicecatalog')
        
        response = sc.describe_product(Name='EMR')
        
        self.emr_sc_product_id = response['ProductViewSummary']['ProductId']
        self.emr_sc_launch_path_id = response['LaunchPaths'][0]['Id']
        self.emr_sc_provisioning_artifact_id = next(
            (item for item in response['ProvisioningArtifacts'] if item['Name'] == self.emr_sc_provisioning_artifact_name), None)['Id']        

    def set_security_groups_subnet(self):
        ec2 = AwsHook(aws_conn_id=self.conn_id).get_client_type('ec2')
        ec2_response = ec2.describe_security_groups(
            Filters=[
                {
                    'Name': 'group-name',
                    'Values': ['ElasticMapReduce-Slave-Private', 'ElasticMapReduce-Master-Private']
                }
            ]
        )
        if not ec2_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('Unable to get service groups: %s' % ec2_response)
        
        self.slave_security_group_id = next((sg for sg in ec2_response['SecurityGroups'] if sg['GroupName'] == 'ElasticMapReduce-Slave-Private'), None)['GroupId']
        self.master_security_group_id = next(sg for sg in ec2_response['SecurityGroups'] if sg['GroupName'] == 'ElasticMapReduce-Master-Private')['GroupId']
        vpc_id = ec2_response['SecurityGroups'][0]['VpcId']

        subnet_response = ec2.describe_subnets(
            Filters=[
                {'Name': 'vpc-id', 'Values': [vpc_id]},
                {'Name': 'state', 'Values': ['available']}
            ]
        )
        if not subnet_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('Unable to get subnet ids: %s' % subnet_response)

        eligible_subnets = [sn for sn in subnet_response['Subnets'] if not sn['MapPublicIpOnLaunch']]
        eligible_subnets.sort(key=lambda item: item['AvailableIpAddressCount'], reverse=True)
        self.subnet_id = eligible_subnets[0].get('SubnetId', None)

        if not self.master_security_group_id or not self.slave_security_group_id or not self.subnet_id:
            raise AirflowException('Unable to find master/slave security group ids or subnet id.')

    def execute(self, context):
        self.set_emr_service_product_ids()
        self.set_security_groups_subnet()

        sc = AwsHook(aws_conn_id=self.conn_id).get_client_type('servicecatalog')
        self.log.info('Provisioning EMR cluster with master SG: %s slave SG: %s and subnet id: %s in %s environment', 
                       self.master_security_group_id, self.slave_security_group_id, self.subnet_id, self.aws_environment)

        response = sc.provision_product(
            ProductId=self.emr_sc_product_id,
            ProvisioningArtifactId=self.emr_sc_provisioning_artifact_id,
            PathId=self.emr_sc_launch_path_id,
            ProvisionedProductName='sc-' + self.emr_cluster_name,
            Tags=self.emr_sc_tags,
            ProvisioningParameters=[
                {
                    'Key': 'ClusterName',
                    'Value': self.emr_cluster_name
                },
                {
                    'Key': 'MasterInstanceType',
                    'Value': self.master_instance_type
                },
                {
                    'Key': 'CoreInstanceType',
                    'Value': self.core_instance_type
                },
                {
                    'Key': 'NumberOfCoreInstances',
                    'Value': self.num_core_nodes
                },
                {
                    'Key': 'EbsRootVolumeSize',
                    'Value': '10'
                },
                {
                    'Key': 'SubnetID',
                    'Value': self.subnet_id
                },
                {
                    'Key': 'MasterSecurityGroupIds',
                    'Value': self.master_security_group_id
                },
                {
                    'Key': 'SlaveSecurityGroupIds',
                    'Value': self.slave_security_group_id
                },
                {
                    'Key': 'ReleaseLabel',
                    'Value': self.emr_release_label
                },
                {
                    'Key': 'CustomJobFlowRoleNameSuffix',
                    'Value': self.emr_custom_job_flow_role_name_suffix
                },
                {
                    'Key': 'CustomServiceRoleNameSuffix',
                    'Value': self.emr_custom_service_role_name_suffix

                },
                {
                    'Key': 'CustomApplicationListJSON',
                    'Value': '''
                    [
                        {"Name": "hadoop"},
                        {"Name": "hive"},
                        {"Name": "pig"},
                        {"Name": "HCatalog"},
                        {"Name": "Spark"}
                    ]
                '''
                },
                {
                    'Key': 'CustomConfigurationsJSON',
                    'Value': '''
                    [
                        {
                            "Classification":"hive-site",
                            "ConfigurationProperties":{
                            "hive.metastore.schema.verification":"false",
                            "hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                            }
                        },
                        {
                            "Classification":"spark-hive-site",
                            "ConfigurationProperties":{
                            "hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                            }
                        },
                        {
                            "Classification":"mapred-site",
                            "ConfigurationProperties":{
                            "mapred.output.direct.EmrFileSystem":"false",
                            "mapred.output.direct.NativeS3FileSystem":"false"
                            }
                        },
                        {
                            "Classification": "hadoop-env",
                            "Configurations": [
                                {
                                    "Classification": "export",
                                    "ConfigurationProperties": {
                                        "HADOOP_NAMENODE_HEAPSIZE": "4096"
                                    }
                                }
                            ]
                        },
                        {
                            "Classification": "hive-env",
                            "Configurations": [
                                {
                                    "Classification": "export",
                                    "ConfigurationProperties": {
                                        "HADOOP_OPTS": "\\"$HADOOP_OPTS -XX:NewRatio=12 -Xms10m -Xmx12288m -XX:MaxHeapFreeRatio=40 -XX:MinHeapFreeRatio=15 -XX:+UseGCOverheadLimit\\"",
                                        "HADOOP_HEAPSIZE": "2048"
                                    }
                                }
                            ]
                        }
                        ]                
                '''
                },
                {
                    'Key': 'BootstrapActionFilePath',
                    'Value': self.emr_sc_bootstrap_file_path
                },
                {
                    'Key': 'StepConcurrencyLevel',
                    'Value': self.emr_step_concurrency
                }
            ]
        )
        self.log.info('SC response: %s', response)   
        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(
                'Service Catalog creation failed: %s' % response)
        else:
            self.log.info('Service Catalog Stack with record id %s created',
                          response['RecordDetail']['RecordId'])
            return response['RecordDetail']['RecordId'], response['RecordDetail']['ProvisionedProductId']

class CreateEMRSensor(BaseSensorOperator):
    
    template_fields = ['provisioned_record_id', 'aws_environment']
    template_ext = ('.json',)

    @apply_defaults
    def __init__(self,
                 *, 
                 conn_id='edt_aws_conn',
                 aws_environment='dev-private',                 
                 provisioned_record_id,
                 **kwargs,
                 ):
        if not provisioned_record_id:
            raise AirflowException("Provisioned service catalog product's record id is required.")
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.aws_environment = aws_environment
        self.provisioned_record_id = provisioned_record_id
        self.target_states = ['SUCCEEDED']
        self.failed_states = ['IN_PROGRESS_IN_ERROR', 'FAILED']
    '''
    def get_boto3_sc_client(self):
        sc_hook = EDTAWSHook(conn_id=self.conn_id, aws_environment=self.aws_environment)
        return sc_hook.get_client_type('servicecatalog')
    '''
    def get_service_catalog_status(self):
        sc_client = AwsHook(aws_conn_id=self.conn_id).get_client_type('servicecatalog')
        self.log.info('Poking service catalog provisioned product with record id %s', self.provisioned_record_id)
        return sc_client.describe_record(Id=self.provisioned_record_id)

    @staticmethod
    def state_from_response(response):
        return response['RecordDetail']['Status']

    @staticmethod
    def failure_message_from_response(response):
        fail_details = response['RecordDetail']['RecordErrors']
        if fail_details:
            return 'for reason(s) {}'.format(fail_details)
        return None

    @staticmethod
    def get_cluster_id_from_response(response):
        outputs = response['RecordOutputs']
        return next((item for item in outputs if item['OutputKey'] == 'ClusterId'), None)['OutputValue']

    def poke(self, context):
        response = self.get_service_catalog_status()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        state = self.state_from_response(response)
        self.log.info('Service catalog product currently %s', state)

        if state in self.target_states:
            cluster_id = self.get_cluster_id_from_response(response)
            context['task_instance'].xcom_push(key='job_flow_id', value=cluster_id)
            return True
        
        if state in self.failed_states:
            # TODO: Delete the SC stack if creation failed
            final_message = 'Service catalog provisioning failed'
            failure_message = self.failure_message_from_response(response)
            if failure_message:
                final_message += ' ' + failure_message
            raise AirflowException(final_message)

        return False

class TerminateEMROperator(BaseOperator):
    """
    Terminate an EMR cluster

    :param aws_conn_id: AWS airflow connection to use, leave null to use AWS default
    :type aws_conn_id: str
    :param region_name: AWS region name, defaulted to us-west-2
    :type region_name: str
    :param job_flow_id: EMR cluster id or job flow id e.g. j-17QAWMPZEL5AE
    :type job_flow_id: str
    """

    template_fields = ['aws_environment', 'provisioned_product_id']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 *,
                 conn_id='edt_aws_conn',
                 aws_environment='dev-private',                 
                 provisioned_product_id,
                 **kwargs
                 ):
        if not provisioned_product_id:
            raise AirflowException('Provisioned product id is required.')
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.aws_environment = aws_environment
        self.provisioned_product_id = provisioned_product_id
    '''
    def get_boto3_sc_client(self):
        sc_hook = EDTAWSHook(conn_id=self.conn_id, aws_environment=self.aws_environment)
        return sc_hook.get_client_type('servicecatalog')
    '''
    def execute(self, context):
        self.log.info('Terminating SC provisioned product %s', self.provisioned_product_id)
        
        # If product was not provisioned there is nothing to terminate
        if not self.provisioned_product_id:
            return
        
        sc_client = AwsHook(aws_conn_id=self.conn_id).get_client_type('servicecatalog')

        response = sc_client.terminate_provisioned_product(
            ProvisionedProductId=self.provisioned_product_id,
            IgnoreErrors=True,
        )

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('Service catalog product termination failed: %s' % response)
        else:
            self.log.info('Service catalog product with id %s terminated', self.provisioned_product_id)
            self.log.info('Service catalog Status %s ', response['RecordDetail']['Status'])
