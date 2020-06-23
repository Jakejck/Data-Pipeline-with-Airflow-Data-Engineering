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
                 aws_credentials_id = "",
                 redshift_conn_id="",
                 load_dim_sql="",
                 table = "",
                 append_data = True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data
        self.sql=load_dim_sql
        self.table = table
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info("Load dimension table")
        if self.append_data == True:
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table
            redshift.run(sql_statement)
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift.run(sql_statement)
        
       
        
