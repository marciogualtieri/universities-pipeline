import os
import logging
import shutil
import pandas as pd
import itertools
from abc import abstractmethod

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils import ToCSVNormalizer

logger = logging.getLogger(__name__)


class CustomBaseOperator(BaseOperator):

    @classmethod
    def _xcom_pull_flatten(cls, context, source_key, source_task_id):
        messages = context['task_instance'].xcom_pull(key=source_key, task_ids=source_task_id)
        messages = cls._flatten_list(messages)
        logger.info('XCOM pulled messages: %s', messages)
        return messages

    @classmethod
    def _flatten_list(cls, values):
        result = []
        if values is None:
            return result
        for value in values:
            if isinstance(value, list):
                result += cls._flatten_list(value)
            else:
                if value is not None:
                    result.append(value)
        return result

    @abstractmethod
    def execute(self, context):
        pass


class FromCSVToNormalizedCSVOperator(CustomBaseOperator):
    @apply_defaults
    def __init__(self, rules, output_folder_path, source_task_id, header, *args, **kwargs):
        super(FromCSVToNormalizedCSVOperator, self).__init__(*args, **kwargs)
        self.rules = rules
        self.output_folder_path = output_folder_path
        self.source_task_id = source_task_id
        self.header = header

    def execute(self, context):
        input_files = self._xcom_pull_flatten(context, 'output', self.source_task_id)
        if not input_files:
            logger.info('No files to normalize')
            return True
        context['task_instance'].xcom_push('input', input_files)
        output_files = []
        logger.info('Normalizing files: %s', input_files)
        for f in input_files:
            output_files.append(ToCSVNormalizer.from_csv(f, self.output_folder_path, self.rules, self.header))
        context['task_instance'].xcom_push('output', output_files)
        logger.info('Normalized files: %s', output_files)
        return True


class FromJSONToNormalizedCSVOperator(CustomBaseOperator):
    @apply_defaults
    def __init__(self, rules, output_folder_path, source_task_id, *args, **kwargs):
        super(FromJSONToNormalizedCSVOperator, self).__init__(*args, **kwargs)
        self.rules = rules
        self.output_folder_path = output_folder_path
        self.source_task_id = source_task_id

    def execute(self, context):
        input_files = self._xcom_pull_flatten(context, 'output', self.source_task_id)
        if not input_files:
            logger.info('No files to normalize')
            return True
        context['task_instance'].xcom_push('input', input_files)
        output_files = []
        logger.info('Normalizing files: %s', input_files)
        for f in input_files:
            output_files.append(ToCSVNormalizer.from_json(f, self.output_folder_path, self.rules))
        context['task_instance'].xcom_push('output', output_files)
        logger.info('Normalized files: %s', output_files)
        return True


class FileMoverOperator(CustomBaseOperator):
    @apply_defaults
    def __init__(self, output_folder_path, source_task_id, source_key, *args, **kwargs):
        super(FileMoverOperator, self).__init__(*args, **kwargs)
        self.output_folder_path = output_folder_path
        self.source_task_id = source_task_id
        self.source_key = source_key

    def execute(self, context):
        files = self._xcom_pull_flatten(context, self.source_key, self.source_task_id)
        if not files:
            logger.info('No files to move')
            return True
        logger.info('Moving files: %s', files)
        for f in files:
            shutil.move(f, os.path.join(self.output_folder_path, os.path.basename(f)))
        return True


class CSVDatabaseLoader(CustomBaseOperator):
    @apply_defaults
    def __init__(self, table_name, connection_id, source_task_id, *args, **kwargs):
        super(CSVDatabaseLoader, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.connection_id = connection_id
        self.source_task_id = source_task_id

    def execute(self, context):
        logger.info("Using connection %s", self.connection_id)
        hook = PostgresHook(postgres_conn_id=self.connection_id)
        engine = hook.get_sqlalchemy_engine()
        files = self._xcom_pull_flatten(context, 'output', self.source_task_id)
        logger.info("Loading files %s into database", files)
        if not files:
            logger.info('No files to load.')
            return True
        for f in files:
            logger.info("Loading CSV file %s into table %s", f, self.table_name)
            data_df = pd.read_csv(f)
            data_df.to_sql(self.table_name, engine, if_exists='append', index=False)
        return True
