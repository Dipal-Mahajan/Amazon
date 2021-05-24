from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

"""

  Common defaults for SuccessNotificationOperator and SnapshotSuccessNotificationOperator

  The end user should pass in at minimum success_file_year and success_file_month as
  this will directly impact how the success flag file is ultimately named.

"""
class BaseSuccessNotification(BaseOperator):
    @apply_defaults
    def __init__(self,
                 success_file_year=None,
                 success_file_month=None,
                 success_file_day=None,
                 success_file_hour=None,
                 host=None,
                 env='dev',
                 endpoint=None,
                 method=None,
                 data=None,
                 params=None,
                 region='us-west-2',
                 aws_service='execute-api',
                 account_id=None,
                 response_check=None,
                 xcom_push=False,
                 log_response=True,
                 *args,
                 **kwargs):
        super(BaseSuccessNotification, self).__init__(*args, **kwargs)
        self.headers = {"Content-Type": "application/json"}
        self.endpoint = endpoint
        self.method = method
        self.host = host
        self.response_check = response_check
        self.xcom_push_flag = xcom_push
        self.log_response = log_response
        if self.host is None:
            if env == 'prod':
                self.host = 'lake.api.gdcorp.tools'
            else:
                self.host = 'lake.api.dev-gdcorp.tools'
        if data is None:
            data = {}
        if 'metadata' not in data:
            metadata = {}
            if success_file_year is not None:
                metadata['year'] = success_file_year
            if success_file_month is not None:
                metadata['month'] = success_file_month
            if success_file_day is not None: 
                metadata['day'] = success_file_day
            if success_file_hour is not None:
                metadata['hour'] = success_file_hour
            if metadata != {}:
                data['metadata'] = metadata
        self.data = data
        self.params = params
        self.auth = BotoAWSRequestsAuth(aws_host=self.host,
                           aws_region=region,
                           aws_service=aws_service)

    def execute(self, context):
        url = 'https://' + self.host + self.endpoint
        self.log.info(f"Calling SuccessNotification:{url} {self.data}")
        response = requests.request(self.method, url, params=self.params, json=self.data, auth=self.auth)
        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        else:
            if response.status_code != 200:
                raise AirflowException(f"HTTP request did not return 200! Code:{response.status_code} text:{response.text}")
        if self.xcom_push_flag:
            return response.text

"""

  This operator will send a success notification after a data set 
  has been written to S3. This will trigger the data lake to add
  update partitions to the metastore and subsequently write a success flag.

"""
class SuccessNotificationOperator(BaseSuccessNotification):
    @apply_defaults
    def __init__(self,
                 db_name,
                 table_name,
                 *args,
                 **kwargs):
        super(SuccessNotificationOperator, self).__init__(*args, **kwargs)
        if self.endpoint is None:
            self.endpoint = f"/v1/databases/{db_name}/tables/{table_name}/scans"
        if self.method is None:
            self.method = "POST"

"""

  This operator will trigger the glue metastore to point to the
  snapshot specified by "location". Once the update has completed,
  a success flag will be generated.

"""
class SnapshotSuccessNotificationOperator(BaseSuccessNotification):
    @apply_defaults
    def __init__(self,
                 db_name,
                 table_name,
                 location,
                 *args,
                 **kwargs):
        super(SnapshotSuccessNotificationOperator, self).__init__(*args, **kwargs)
        self.data['location'] = location
        if self.endpoint is None:
            self.endpoint = f"/v1/databases/{db_name}/tables/{table_name}"
        if self.method is None:
            self.method = "PATCH"

