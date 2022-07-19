#!/bin/bash
docker exec -it project2_spark_1 /../../opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/starter/sparkpyeventskafkastreamtoconsole.py | tee ../spark/logs/eventstream.log 
