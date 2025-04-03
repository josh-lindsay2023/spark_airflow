from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,

                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 tables = None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data Quality Check failed for {table} and returned no rows')
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f'Data Quality Check failed for {table} and returned no rows')
            self.log.info(f'Data quality checks passed for {table} and returned {num_records} rows')









