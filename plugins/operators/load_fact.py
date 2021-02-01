from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Data Operator that loads the data from the staging table into the 
    fact table.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_connection_id,
                 table_target,
                 query,
                 *args, **kwargs):
        """
        Class constructor.
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.target_table = table_target
        self.query = query
        self.redshift_connection_id = redshift_connection_id
        self.autocommit = True

    def execute(self, context):
        """
        Executes the Insert.
        """

        self.log.info(f'Start INSERT into {self.target_table}')
        self.hook = PostgresHook(postgres_connection_id=self.redshift_connection_id)
        insert_sql = f"""
            INSERT INTO {self.target_table}
            {self.query};
            COMMIT;
        """
        self.hook.run(insert_sql, self.autocommit)
        self.log.info(f"Data added into {self.target_table} complete.")