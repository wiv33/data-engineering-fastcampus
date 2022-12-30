#!/bin/zsh

# data-engineering-fastcampus
bin/spark-submit \
        --master k8s://https://node1:6443 \
        --deploy-mode cluster \
        --name spark-pi \
        --class org.apache.spark.examples.SparkPi \
        --conf spark.executor.instances=5 \
        --conf spark.kubernetes.namespace=spark \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
        --conf spark.kubernetes.container.image=psawesome/spark:3.3.1 \
        local:///opt/spark/examples/jars/*.jar


# pyspark
bin/spark-submit \
        --master k8s://https://node1:6443 \
        --deploy-mode cluster \
        --name spark-pi \
        --conf spark.executor.instances=5 \
        --conf spark.kubernetes.namespace=spark \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
        --conf spark.kubernetes.container.image=bitnami/spark-py:3 \
        local:///opt/spark/examples/src/main/python/pi.py