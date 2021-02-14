import os
import logging
import re

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.sensors.base import BaseSensorOperator

logger = logging.getLogger(__name__)


class CustomFileSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, folder_path, file_pattern, *args, **kwargs):
        super(CustomFileSensor, self).__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.file_pattern = file_pattern

    def poke(self, context):
        files = [os.path.join(self.folder_path, f)
                 for f in os.listdir(self.folder_path)
                 if re.match(self.file_pattern, f)]
        if not files:
            logger.info('No files found for folder %s and pattern %s', self.folder_path, self.file_pattern)
            return True
        context['task_instance'].xcom_push('output', files)
        logger.info('Found files: %s', files)
        return True


class CustomFileSensorPlugin(AirflowPlugin):
    name = "custom_file_sensor_plugin"
    operators = [CustomFileSensor]
