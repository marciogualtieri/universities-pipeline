from airflow.models import DagBag
import unittest


class TestDag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dag_bag = DagBag()

    def test_dag_loaded(self):
        dag = self.dag_bag.get_dag(dag_id='universities')
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 10)

    def test_dag_structure(self):
        """
        I don't find this test necessarily useful in this current setup, but will keep it for future reference.
        """
        dag = self.dag_bag.get_dag(dag_id='universities')
        self.assertDagDictEqual(
            {
                'start': {'uni2_file_sensor', 'uni3_file_sensor', 'uni1_file_sensor'},
                'stop': {},
                'uni1_file_sensor': {'uni1_file_normalizer'},
                'uni1_file_normalizer': {'csv_database_loader', 'processed_files_mover'},
                'uni2_file_sensor': {'uni2_file_normalizer'},
                'uni2_file_normalizer': {'csv_database_loader', 'processed_files_mover'},
                'uni3_file_sensor': {'uni3_file_normalizer'},
                'uni3_file_normalizer': {'csv_database_loader', 'processed_files_mover'},
                'processed_files_mover': {'stop'},
                'csv_database_loader': {'stop'}
            },
            dag)

    def assertDagDictEqual(self, source, dag):
        self.assertEqual(dag.task_dict.keys(), source.keys())
        for task_id, downstream_list in source.items():
            self.assertTrue(dag.has_task(task_id))
            task = dag.get_task(task_id)
            self.assertEqual(task.downstream_task_ids, set(downstream_list))
