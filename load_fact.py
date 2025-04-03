from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql='',
                 table='',
                 truncate= False,
                 *args, **kwargs):


        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id,
        self.sql = sql
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            redshift.run(f'TRUNCATE {self.table}')
        redshift.run(f'INSERT INTO {self.table} {self.sql}')
        self.log.info(f'Fact {self.table} loaded correctly')


        
