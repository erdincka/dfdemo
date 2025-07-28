sources = [
    'file',
    'api',
    'sales',
    'kafkatopic',
]

targets = [
    's3',
    'posix',
    # 'hive',
    # 'iceberg',
    # 'kafkatopic',
]

write_as = [
    'csv',
    'json',
    'parquet',
]

# locations = [
#     'volume',
#     'db',
#     'bucket',
# ]