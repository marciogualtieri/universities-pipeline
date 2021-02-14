import csv
from unittest import TestCase
import os
from pathlib import Path
import random
import string
import shutil

from universities import utils


class TestUtils(TestCase):

    def setUp(self):
        self.test_output_folder = self._generate_folder_name('/tmp')
        self._create_folder(self.test_output_folder)

    def tearDown(self):
        self._delete_folder(self.test_output_folder)

    def test_to_csv_normalizer_from_csv_with_mapper(self):
        test_file = 'uni1.csv'
        expected_file = 'uni1_normalized.csv'
        rules = {
            'first_name': {
                'column': 0,
                'mapper': utils.MapperGenerator.generate_copy_mapper(),
                'filter': None
            },
            'last_name': {
                'column': 1,
                'mapper': utils.MapperGenerator.generate_copy_mapper(),
                'filter': None
            },
            'subject': {
                'column': 2,
                'mapper': utils.MapperGenerator.generate_copy_mapper(),
                'filter': None
            },
            'grade': {
                'column': 3,
                'mapper': utils.MapperGenerator.generate_copy_mapper(),
                'filter': None
            },
        }

        utils.ToCSVNormalizer.from_csv(self._get_resource_full_path(test_file), self.test_output_folder, rules, 0)
        self.assertEqual(self._csv_file_to_matrix(self._get_resource_full_path(expected_file)),
                         self._csv_file_to_matrix(self._get_output_full_path(test_file)))

    def test_to_csv_normalizer_from_csv_with_mapper_and_filter(self):
        test_file = 'uni2.csv'
        expected_file = self._get_resource_full_path('uni2_normalized.csv')
        copy_mapper_function = utils.MapperGenerator.generate_regex_mapper(r'(.*)', r'\1')
        rules = {
            'first_name': {
                'column': 0,
                'mapper': utils.MapperGenerator.generate_regex_mapper(r'([^_]+)__([^_]+)', r'\1'),
                'filter': utils.FilterGenerator.generate_regex_filter(r'(?!name)')
            },
            'last_name': {
                'column': 0,
                'mapper': utils.MapperGenerator.generate_regex_mapper(r'([^_]+)__([^_]+)', r'\2'),
                'filter': None
            },
            'subject': {
                'column': 1,
                'mapper': utils.MapperGenerator.generate_copy_mapper(),
                'filter': None
            },
            'grade': {
                'column': 2,
                'mapper': utils.MapperGenerator.generate_copy_mapper(),
                'filter': None
            },
        }

        utils.ToCSVNormalizer.from_csv(self._get_resource_full_path(test_file), self.test_output_folder, rules, 0)
        self.assertEqual(self._csv_file_to_matrix(self._get_resource_full_path(expected_file)),
                         self._csv_file_to_matrix(self._get_output_full_path(test_file)))

    def test_to_json_normalizer_from_json(self):
        test_file = 'uni3.json'
        expected_file = self._get_resource_full_path('uni3_normalized.csv')
        copy_mapper_function = utils.MapperGenerator.generate_regex_mapper(r'(.*)', r'\1')
        rules = {
            'first_name': {
                'column': 'first_name',
                'mapper': utils.MapperGenerator.generate_regex_mapper(r'([^_]+)__([^_]+)', r'\1'),
                'filter': None
            },
            'last_name': {
                'column': 'last_name',
                'mapper': utils.MapperGenerator.generate_regex_mapper(r'([^_]+)__([^_]+)', r'\2'),
                'filter': None
            },
            'subject': {
                'column': 'subject',
                'mapper': copy_mapper_function,
                'filter': None
            },
            'grade': {
                'column': 'grade',
                'mapper': copy_mapper_function,
                'filter': None
            },
        }

        utils.ToCSVNormalizer.from_json(self._get_resource_full_path(test_file), self.test_output_folder, rules)
        self.assertEqual(self._csv_file_to_matrix(self._get_resource_full_path(expected_file)),
                         self._csv_file_to_matrix(self._get_output_full_path('uni3.csv')))

    @staticmethod
    def _csv_file_to_matrix(file_path):
        with open(file_path, newline='') as f:
            reader = csv.reader(f)
            return list(reader)

    @staticmethod
    def _get_resource_full_path(resource_name):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources', resource_name)

    def _get_output_full_path(self, expectation_name):
        return os.path.join(self.test_output_folder, expectation_name)

    @staticmethod
    def _create_folder(folder_path):
        Path(folder_path).mkdir(parents=True)

    @staticmethod
    def _delete_folder(folder_path):
        shutil.rmtree(folder_path)

    @staticmethod
    def _generate_folder_name(base_folder_path, length=5):
        letters = string.ascii_letters
        return  os.path.join(base_folder_path,
                             os.path.basename(__file__) + ''.join(random.choice(letters) for _ in range(length)))

    @staticmethod
    def _get_file_name_without_extension(file_path):
        return ''.join(os.path.splitext(os.path.basename(file_path))[:-1])
