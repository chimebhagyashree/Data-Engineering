from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
    Description:
    - Checks Data quality on Dimension Tables
    - Focuses on NULL value entry on primary key of tables
    
    '''
    data_quality_checks = [
        {'check_sql' : "SELECT COUNT(*) FROM users WHERE userid IS NULL",
         'expected_val' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM songs WHERE songid IS NULL",
         'expected_val' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM time WHERE start_time IS NULL",
         'expected_val' : 0}]
        
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failureCount = 0
        failingTest = []
        
        for check in DataQualityOperator.data_quality_checks:
            sql_query = check.get('check_sql')
            expect_result = check.get('expected_val')
            records = redshift_hook.get_records(sql_query)[0]
            
            if expect_result != records[0]:
                failureCount += 1
                failingTest.append(sql_query)
                
            if failureCount > 0:
                self.log.info("bad data quality")
                self.log.info("number of failed test: {}".format(failureCount))
                self.log.info(failingTest)
                raise ValueError("Data Quality check Failed")
                
            if failureCount == 0:
                self.log.info("Pass")