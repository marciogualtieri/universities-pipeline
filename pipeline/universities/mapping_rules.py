from utils import MapperGenerator, FilterGenerator

uni1_rules = {
    'first_name': {
        'column': 0,
        'mapper': MapperGenerator.generate_copy_mapper(),
        'filter': None
    },
    'last_name': {
        'column': 1,
        'mapper': MapperGenerator.generate_copy_mapper(),
        'filter': None
    },
    'subject': {
        'column': 2,
        'mapper': MapperGenerator.generate_copy_mapper(),
        'filter': None
    },
    'grade': {
        'column': 3,
        'mapper': MapperGenerator.generate_copy_mapper(),
        'filter': None
    }
}

uni2_rules = {
    'first_name': {
        'column': 0,
        'mapper': MapperGenerator.generate_regex_mapper(r'([^_]+)__([^_]+)', r'\1'),
        'filter': FilterGenerator.generate_regex_filter(r'(?!name)')
    },
    'last_name': {
        'column': 0,
        'mapper': MapperGenerator.generate_regex_mapper(r'([^_]+)__([^_]+)', r'\2'),
        'filter': None
    },
    'subject': {
        'column': 1,
        'mapper': MapperGenerator.generate_copy_mapper(),
        'filter': None
    },
    'grade': {
        'column': 2,
        'mapper': MapperGenerator.generate_copy_mapper(),
        'filter': None
    }
}

uni3_rules = {
    'first_name': {
        'column': 'first_name',
        'mapper': MapperGenerator.generate_regex_mapper(r'([^_]+)__([^_]+)', r'\1'),
        'filter': None
    },
    'last_name': {
        'column': 'last_name',
        'mapper': MapperGenerator.generate_regex_mapper(r'([^_]+)__([^_]+)', r'\2'),
        'filter': None
    },
    'subject': {
        'column': 'subject',
        'mapper': MapperGenerator.generate_copy_mapper(),
        'filter': None
    },
    'grade': {
        'column': 'grade',
        'mapper': MapperGenerator.generate_copy_mapper(),
        'filter': None
    },
}
