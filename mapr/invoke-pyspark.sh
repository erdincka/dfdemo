#!/bin/bash

# Run PySpark with Iceberg packages and python file
/opt/mapr/spark/spark-3.5.5/bin/pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2 < /home/mapr/ingest_to_iceberg.py