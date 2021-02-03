from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        TIMEFORMAT AS 'epochmillisecs'
        REGION '{}'
    """
    
    copy_sql_from_date = """
        COPY {} 
        FROM '{}/{}/{}/'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        TIMEFORMAT AS 'epochmillisecs'
        REGION '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_conn_id="",
                 table = "",
                 s3_path = "",
                 data_format = "",
                 region= "us-west-2",                 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.data_format = data_format
        self.execution_date = kwargs.get('execution_date')
        
        
    def execute(self, context):
        '''
            Executes staging of json files from S3 into Redshift.
        '''
        
        aws_hook = AwsHook(self.aws_conn_id)
        
        self.log.info("Getting AWS credentials...")
        
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)   

        # Backfill a specific date
        if self.execution_date:
            formatted_query = StageToRedshiftOperator.copy_sql_from_date.format(
                self.table, 
                self.s3_path,
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key, 
                self.data_format,
                self.region
            )
        else:
            formatted_query = StageToRedshiftOperator.copy_sql.format(
                self.table, 
                self.s3_path, 
                credentials.access_key,
                credentials.secret_key, 
                self.data_format,
                self.region
            )
        
        self.log.info(f"Copying {self.table} data from S3 to Redshift...")
        
        redshift.run(formatted_query)
        
        self.log.info(f"Copying of table {self.table} from S3 to Redshift complete...")
