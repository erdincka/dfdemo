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

CLUSTER_NAME = "maprdemo.mapr.io"
MOUNT_PATH = f"/mapr/{CLUSTER_NAME}"
DEMO_VOLUME = "demovol"
DEMO_STREAM = "/demovol/demostream"

DEMO_FOLDERS = [
    "/",
    f"/{DEMO_VOLUME}",
    f"/{DEMO_VOLUME}/users",
    "/tenant1",
    "/tenant1/user11",
    "/tenant1/user12",
    "/tenant2",
    "/tenant2/user21",
]
