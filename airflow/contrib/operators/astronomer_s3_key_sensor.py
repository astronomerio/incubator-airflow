import os
import logging

from airflow.operators.sensors import BaseSensorOperator
from airflow.hooks import S3Hook
from airflow.utils.decorators import apply_defaults
from boto.s3.connection import S3Connection
from airflow.hooks.base_hook import CONN_ENV_PREFIX
from urllib import quote_plus

aws_key = os.getenv('AWS_ACCESS_KEY_ID', '')
aws_secret = quote_plus(os.getenv('AWS_SECRET_ACCESS_KEY', ''))
os.environ[CONN_ENV_PREFIX + 'S3_CONNECTION'] = 's3://{aws_key}:{aws_secret}@S3'.format(**locals())

class AstronomerS3KeySensor(BaseSensorOperator):

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self,
            bucket_key,
            bucket_name,
            *args, **kwargs):
        super(AstronomerS3KeySensor, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key

    def poke(self, context):
        hook = S3Hook(s3_conn_id='S3_CONNECTION')
        full_url = "s3://" + self.bucket_name + "/" + self.bucket_key
        logging.info('Poking for key : {full_url}'.format(**locals()))
        return hook.check_for_prefix(prefix=self.bucket_key,
                                     bucket_name=self.bucket_name,
                                     delimiter='/')
