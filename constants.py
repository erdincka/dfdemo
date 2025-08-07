sources = [
    'File',
    'NASA API',
    'Sales DB',
    'Stream',
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

DEMO_STREAM = '/demovol/demostream'
