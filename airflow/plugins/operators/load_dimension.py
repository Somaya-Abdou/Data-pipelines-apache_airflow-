from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """INSERT INTO {} {}  """
    @apply_defaults
    def __init__(self,
                # conn_id = your-connection-name
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                append ="",
                sql_query = "",
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table=table
        self.append=append 
        self.sql_query=sql_query
    
    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
          
        if self.append == "True" :
            self.log.info('appending to destination Redshift table')
            redshift.run(LoadDimensionOperator.insert_sql.format(self.table, self.sql_query) ) 
        else :    
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info('inserting into destination Redshift table')
            redshift.run(LoadDimensionOperator.insert_sql.format(self.table, self.sql_query) ) 