from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

        """
    Construct dimension tables
    
    parameters:
    1.redshift_conn_id
    2.Table : Target staging table in Redshift to copy data into
    3. Insert_sql: sql statement used to extract dimension tables

    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 insert_sql,
                 append_only= False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql
        self.append_only = append_only
        
    def execute(self, context):
        # connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info("Delete {} dimension table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))         
            
        self.log.info("Insert data from staging table into {} dimension table".format(self.table))

        # build insert statement
        table_insert_sql = f"INSERT INTO {self.table_name} {self.insert_sql}"

        redshift_hook.run(table_insert_sql)
        self.log.info(f"Successfully completed insert into {self.table}")