from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 dq_checks=[],
                 expected_result=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.dq_checks=dq_checks
        self.expected_result=expected_result

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if len(self.dq_checks)<=0:
            self.log.info('Data Quality not passed')
            return
        
        failed_queries=[]
        
        for sql in self.dq_checks:
            try:
                self.log.info(f"Executing Query: {sql}")
                records = redshift_hook.get_records(sql)[0][0]
            except Exception as e:
                self.log.info(f"Execution failed with exception: {e}")

            if self.expected_result != records:
                failed_queries.append(sql)

        if len(failed_queries) > 0:
            self.log.info('Results not passed')
            self.log.info(failed_queries)
            raise ValueError('Data quality checks failed')
        else:
            self.log.info("All data quality tests passed")