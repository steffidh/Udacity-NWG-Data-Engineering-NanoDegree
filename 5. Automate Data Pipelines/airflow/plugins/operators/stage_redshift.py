from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 destination_table = '',
                 json_paths = '',
                 s3_bucket = '',
                 s3_key = '',
                 aws_region = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.destination_table = destination_table
        self.json_paths = json_paths
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_region = aws_region

    def execute(self, context):
        self.log.info('StageToRedshiftOperator - Loading Data from S3 to Redshift')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading data from {self.s3_bucket}/{self.s3_key} to Redshift {self.destination_table} table")
        
        copy_query = f"""
        COPY {self.destination_table}
        FROM 's3://{self.s3_bucket}/{self.s3_key}'
        ACCESS_KEY_ID '{credentials.access_key}'
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        JSON '{self.json_paths}'
        COMPUPDATE OFF;
        """





