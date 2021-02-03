from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_query_trunc = """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
        COMMIT;
    """
    
    insert_query_append = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_query="",
                 append_only = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_statement = load_query
        self.append_only = append_only

    def execute(self, context):
        '''
            Loads dimension table into Redshift.
        '''
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Loading dimension table {self.table} into Redshift")
        
        if (not self.append_only):
            formatted_query = LoadDimensionOperator.insert_query_trunc.format(
                self.table,
                self.table,
                self.load_sql_statement
            )
        else:
            formatted_query = LoadDimensionOperator.insert_query_append.format(
                self.table,
                self.load_sql_statement
            )
        
        redshift.run(formatted_query)
        
        self.log.info(f"Loading dimension table {self.table} complete")
