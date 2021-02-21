import unittest
from airflow.utils.state import State
from pipeline.universities.sensors import CustomFileSensor
from tests.base_test_case import BaseTestCase
import airflow
from airflow.models.taskinstance import TaskInstance

DEFAULT_DATE = airflow.utils.dates.days_ago(1)
TEST_DAG_ID = 'test_custom_file_sensor'
TEST_TASK_ID = 'test_custom_file_sensor'


class TestCustomFileSensor(BaseTestCase):

    def setUp(self):
        self.dag = airflow.DAG(TEST_DAG_ID, schedule_interval='@once', default_args={'start_date': DEFAULT_DATE})
        self.operator = CustomFileSensor(
            dag=self.dag,
            task_id='test_custom_file_sensor',
            poke_interval=1,
            folder_path=self._get_resource_full_path(""),
            file_pattern='uni1.csv',
        )
        self.task_instance = TaskInstance(task=self.operator, execution_date=DEFAULT_DATE)

    def test_execute_no_trigger(self):
        self.task_instance.run(ignore_ti_state=True, test_mode=True)
        self.assertEqual(self.task_instance.state, State.SUCCESS)
        context = self.task_instance.get_template_context()
        messages = context['task_instance'].xcom_pull(key='output', task_ids=TEST_TASK_ID)
        self.assertEqual(messages, [self._get_resource_full_path("uni1.csv")])
