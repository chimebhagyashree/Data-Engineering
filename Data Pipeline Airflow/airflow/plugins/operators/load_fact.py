from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''
    Description:
    - Data will be loaded into Fact table in append mode

    '''
    insert_sql = """
    INSERT INTO {} {};
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table = "",
                 append_data = True,
                 sql_query = "",
                 *args, **kwargs):
        
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_data = append_data
        self.sql_query = sql_query
        
    def execute(self, context):
        self.log.info(f"Start LoadFactorOperator on {self.table}.{self.append_data}. appending data")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        redshift_hook.run(LoadFactOperator.insert_sql.format(self.table, self.sql_query))
                              
        self.log.info(f"Finished Loading fact table '{self.table}' into Redshift")