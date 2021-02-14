from airflow.operators.dummy import DummyOperator
from datetime import timedelta
import airflow

from sensors import CustomFileSensor
from operators import FromCSVToNormalizedCSVOperator, FromJSONToNormalizedCSVOperator
from operators import FileMoverOperator, CSVDatabaseLoader
import mapping_rules

# TODO: Use Airflow configuration variables instead of hard-coded values
INCOMING_FILES_PATH = '/tmp/incoming'
NORMALIZED_FILES_PATH = '/tmp/normalized'
PROCESSED_FILES_PATH = '/tmp/processed'
CONNECTION_ID = 'universities'
TABLE_NAME = 'raw_grades'

default_args = {
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with airflow.DAG("universities", default_args=default_args, schedule_interval="@hourly") as dag:
    start_task = DummyOperator(task_id="start")

    stop_task = DummyOperator(task_id="stop")

    uni1_file_sensor_task = CustomFileSensor(
        task_id="uni1_file_sensor",
        poke_interval=10,
        folder_path=INCOMING_FILES_PATH,
        file_pattern=r'uni1.csv')

    uni1_file_normalizer_task = FromCSVToNormalizedCSVOperator(
        task_id='uni1_file_normalizer',
        output_folder_path=NORMALIZED_FILES_PATH,
        header=0,
        rules=mapping_rules.uni1_rules,
        source_task_id='uni1_file_sensor')

    uni2_file_sensor_task = CustomFileSensor(
        task_id="uni2_file_sensor",
        poke_interval=10,
        folder_path=INCOMING_FILES_PATH,
        file_pattern=r'uni2.csv')

    uni2_file_normalizer_task = FromCSVToNormalizedCSVOperator(
        task_id='uni2_file_normalizer',
        output_folder_path=NORMALIZED_FILES_PATH,
        header=0,
        rules=mapping_rules.uni2_rules,
        source_task_id='uni2_file_sensor')

    uni3_file_sensor_task = CustomFileSensor(
        task_id="uni3_file_sensor",
        poke_interval=10,
        folder_path=INCOMING_FILES_PATH,
        file_pattern=r'uni3.json')

    uni3_file_normalizer_task = FromJSONToNormalizedCSVOperator(
        task_id='uni3_file_normalizer',
        output_folder_path=NORMALIZED_FILES_PATH,
        rules=mapping_rules.uni3_rules,
        source_task_id='uni3_file_sensor')

    processed_files_mover = FileMoverOperator(
        task_id='processed_files_mover',
        output_folder_path=PROCESSED_FILES_PATH,
        source_task_id=['uni1_file_normalizer', 'uni2_file_normalizer', 'uni3_file_normalizer'],
        source_key='input')

    csv_database_loader = CSVDatabaseLoader(
        task_id='csv_database_loader',
        connection_id=CONNECTION_ID,
        table_name=TABLE_NAME,
        source_task_id=['uni1_file_normalizer', 'uni2_file_normalizer', 'uni3_file_normalizer'],
    )

start_task >> uni1_file_sensor_task >> uni1_file_normalizer_task >> [processed_files_mover, csv_database_loader] >> stop_task

start_task >> uni2_file_sensor_task >> uni2_file_normalizer_task >> [processed_files_mover, csv_database_loader] >> stop_task

start_task >> uni3_file_sensor_task >> uni3_file_normalizer_task >> [processed_files_mover, csv_database_loader] >> stop_task
