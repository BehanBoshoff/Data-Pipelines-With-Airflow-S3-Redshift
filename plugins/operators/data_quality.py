from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info(f"Data Quality check for {table['name']} table in progress.")
            
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table['name']}")
            num_records = records[0][0]
            
            if len(records) < table['expected_result'] or len(records[0]) < table['expected_result']:
                raise ValueError(f"Data quality check failed. {table['name']} returned no results.")                                       
            elif num_records < table['expected_result']:
                raise ValueError(f"Data quality check failed. {table['name']} contained 0 rows.")             
            else:
                self.log.info(f"Data quality check on table {table['name']} passed with {records[0][0]} records.")