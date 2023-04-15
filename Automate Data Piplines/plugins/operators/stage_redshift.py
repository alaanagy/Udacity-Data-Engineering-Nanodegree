from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    
    """
    Copies JSON data from S3 to staging tables in Redshift data warehouse
    
    parameters:
    1.redshift_conn_id
    2.aws_credentials_id
    3.Table : Target staging table in Redshift to copy data into
    4.s3_bucket: S3 bucket where JSON data resides
    5.region: AWS Region where the source data is located 
    
    """
    
    ui_color = '#358140'
    
    
    copy_sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 s3_path = '',
                 region = '',
                 json_path = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table           
        self.s3_path = s3_path
        self.region = region
        self.json_path = json_path
        

    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")      


        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table, 
                self.s3_path, 
                credentials.access_key,
                credentials.secret_key, 
                self.json_path              
        )
        
        redshift.run(formatted_sql)