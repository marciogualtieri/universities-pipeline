import os
import re

import pandas as pd


class MapperGenerator:

    @staticmethod
    def generate_regex_mapper(extract_regex, replace_regex):
        return lambda value: re.sub(extract_regex, replace_regex, str(value).strip())

    @staticmethod
    def generate_copy_mapper():
        return lambda value: str(value).strip()


class FilterGenerator:

    @staticmethod
    def generate_regex_filter(regex):
        return lambda value: bool(re.match(regex, str(value).strip()))


class ToCSVNormalizer:

    @classmethod
    def normalized_df(cls, data_df, rules, file_path):
        university = cls.get_file_name_without_extension(file_path)
        data_df = cls.apply_mappers(data_df, rules)
        data_df = cls.apply_filters(data_df, rules)
        data_df['university'] = university
        return data_df

    @classmethod
    def from_csv(cls, file_path, destination_folder_path, rules, header):
        data_df = pd.read_csv(file_path, header=header)
        data_df = cls.normalized_df(data_df, rules, file_path)
        output_file = os.path.join(destination_folder_path, os.path.basename(file_path))
        data_df.to_csv(output_file, index=False)
        return output_file

    @classmethod
    def from_json(cls, file_path, destination_folder_path, rules):
        data_df = pd.read_json(file_path)
        data_df = cls.normalized_df(data_df, rules, file_path)
        output_file = os.path.join(destination_folder_path, f'{cls.get_file_name_without_extension(file_path)}.csv')
        data_df.to_csv(output_file, index=False)
        return output_file

    @staticmethod
    def apply_filters(data_df, rules):
        result_df = data_df
        for column in rules.keys():
            source_column = rules[column]['column']
            filter_function = rules[column]['filter']
            if filter_function:
                if isinstance(source_column, int):
                    result_df = result_df[result_df.iloc[:, source_column].apply(filter_function)]
                else:
                    result_df = result_df[result_df[source_column].apply(filter_function)]
        return result_df

    @staticmethod
    def apply_mappers(data_df, rules):
        result_df = pd.DataFrame(columns=rules.keys())
        for column in rules.keys():
            source_column = rules[column]['column']
            mapper_function = rules[column]['mapper']
            if isinstance(source_column, int):
                result_df[column] = data_df.iloc[:, source_column].apply(mapper_function)
            else:
                result_df[column] = data_df[source_column].apply(mapper_function)
        return result_df

    @staticmethod
    def get_file_name_without_extension(file_path):
        return ''.join(os.path.splitext(os.path.basename(file_path))[:-1])
