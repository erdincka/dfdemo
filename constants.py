sources = [
    'file',
    'api',
    'sales',
]

targets = [
    's3',
    'posix',
    'hive',
    'iceberg',
    'kafkatopic'
]

write_as = [
    'json',
    'parquet',
]

locations = [
    'volume',
    'db',
    'bucket',
]