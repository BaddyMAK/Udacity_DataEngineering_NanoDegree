from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins.helpers.sql_queries import SqlQueries


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, redshift_conn_id, *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs):
        self.redshift_conn_id=redshift_conn_id,

    def execute(self, context):
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        load_user_table_sql = LoadDimensionOperator.SqlQueries.user_table_insert
        load_song_table_sql = LoadDimensionOperator.SqlQueries.song_table_insert
        load_artist_table_sql = LoadDimensionOperator.SqlQueries.artist_table_insert
        load_time_table_sql = LoadDimensionOperator.SqlQueries.time_table_insert
        redshift_hook.run(load_user_table_sql)
        redshift_hook.run(load_song_table_sql)
        redshift_hook.run(load_artist_table_sql)
        redshift_hook.run(load_time_table_sql)
