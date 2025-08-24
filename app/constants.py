sources = [
    "File",
    "REST API",
    # "Sales DB",
    "Stream",
]

targets = [
    "s3",
    "posix",
    # 'hive',
    # 'iceberg',
    # 'kafkatopic',
]

write_as = [
    "csv",
    "json",
    "parquet",
]

# locations = [
#     'volume',
#     'db',
#     'bucket',
# ]

DEMO_STREAM = "/demovol/demostream"

DEMO_FOLDERS = [
    "/",
    "/user/mapr/users",
    "/demovol",
    "/tenant1",
    "/tenant1/user11",
    "/tenant1/user12",
    "/tenant2",
    "/tenant2/user21",
]
