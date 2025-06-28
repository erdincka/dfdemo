services = [
    { 'name': 'LLM', 'command': 'echo will be running vllm'},
]

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
]

write_as = [
    'json',
    'parquet',
]
