from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "redshift",
                 target_db = "",
                 destination_table = "",
                 sql = "",
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.target_db = target_db
        self.destination_table = destination_table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        ##self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.destination_table}")
            
        sql = "\nINSERT INTO {target_db}.{destination_table} ({sql})".format(target_db=self.target_db,destination_table=self.destination_table,sql=self.sql)
        self.log.info("SQL is running: {sql}".format(sql=sql))
        redshift.run(sql)
        self.log.info("LoadDimOperator ran Successfully")
        
