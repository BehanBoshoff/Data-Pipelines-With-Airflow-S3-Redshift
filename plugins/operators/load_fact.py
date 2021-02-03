from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_query = """
        INSERT INTO {}
        {};
        COMMIT;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_statement = load_query

    def execute(self, context):
        '''
            Loads fact table into Redshift.
        '''
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading fact table in Redshift")
            
        formatted_query = LoadFactOperator.insert_query.format(
            self.table,
            self.load_sql_statement
        )
        
        redshift.run(formatted_query)