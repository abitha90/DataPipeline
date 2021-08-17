from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    facts_sql_template = """
    CREATE TABLE IF NOT EXISTS {destination_table} AS
    {sql_statement}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 destination_table="",
                 sql_statement='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_statement=sql_statement

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        facts_sql = FactsCalculatorOperator.facts_sql_template.format(
            destination_table=self.destination_table,
            sql_statement=self.sql_statement
        )
        redshift.run(facts_sql)
        self.log.info('LoadFactOperator executed')
