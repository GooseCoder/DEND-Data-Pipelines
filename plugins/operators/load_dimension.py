from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Data Operator that loads the data from the staging table and inserts it 
    into the dimension table.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_connection_id,
                 table_target,
                 query,
                 truncate_before = True,
                 *args, **kwargs):
        """
        Class constructor.
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.target_table = table_target
        self.redshift_connection_id = redshift_connection_id
        self.query = query
        self.autocommit = True
        self.truncate_before = truncate_before

    def execute(self, context):
        """
        Loads the dimension into the target table.
        """

        self.log.info(f'Loading dimension {self.target_table}.')
        self.hook = PostgresHook(postgres_connection_id=self.redshift_connection_id)
        if self.truncate_before:
            self.log.info('Truncating')
            sql_query = f"""
                TRUNCATE TABLE {self.target_table};
                INSERT INTO {self.target_table}
                {self.query};
                COMMIT;
            """
        else:
            self.log.info('Inserting')
            sql_query = f"""
                INSERT INTO {self.target_table}
                {self.query};
                COMMIT;
            """
        self.hook.run(sql_query, self.autocommit)
        self.log.info(f"Table {self.target_table} loaded.")
