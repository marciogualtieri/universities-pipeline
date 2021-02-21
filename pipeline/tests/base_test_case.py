import csv
import os
import random
import shutil
import string
from pathlib import Path
from unittest import TestCase


class BaseTestCase(TestCase):

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
