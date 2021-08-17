from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    dimensions_sql_template = """
    DROP TABLE IF EXISTS {destination_table} 
    CREATE TABLE IF NOT EXISTS {destination_table} AS
    {sql_statement};
    """
    dimension_table_truncate = """
    TRUNCATE TABLE {destination_table} 
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 destination_table="",
                 sql_statement="",
                 append_data=False,
                 *args, **kwargs):

        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_statement=sql_statement

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == True:
            sql_statement = LoadDimensionOperator.dimensions_sql_template.format(
            destination_table=self.destination_table,
            sql_statement=self.sql_statement
            )
            redshift.run(sql_statement)
        else:
            sql_statement = LoadDimensionOperator.dimension_table_truncate.format(
            destination_table=self.destination_table
            )+LoadDimensionOperator.dimensions_sql_template.format(
            destination_table=self.destination_table,
            sql_statement=self.sql_statement
            )
            redshift.run(sql_statement)
        self.log.info('LoadDimensionOperator executed')
