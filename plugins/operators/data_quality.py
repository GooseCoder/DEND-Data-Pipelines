from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Data Operator that checks tha quality of the inserted data.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_connection_id,
                 tables,
                 *args, **kwargs):
        """
        Class constructor.
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables_to_check = tables
        self.redshift_connection_id = redshift_connection_id

    def execute(self, context):
        """
        Checks data quality.
        """

        self.hook = PostgresHook(postgres_connection_id=self.redshift_connection_id)
        for table in self.tables_to_check:
            self.log.info(f"Starting check on table {table}")
            rows = self.hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(rows) < 1 or len(rows[0]) < 1:
                raise ValueError(f"Quality check failed. {table} has no results")
            num_records = rows[0][0]
            if num_records < 1:
                raise ValueError(f"Quality check failed. {table} is empty")
            self.log.info(f"Quality check on table {table} passed, it has {rows[0][0]} rows")