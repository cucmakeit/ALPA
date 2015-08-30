#! /bin/bash

spark-submit --class com.wtist.spark.graphx.ALPAtest.ALPA \
    --master yarn-cluster \
    --num-executors 8 \
    --driver-memory 10g \
    --executor-memory 20g \
    --executor-cores 8 \
    target/scala-2.10/alpa_2.10-1.0.jar \
    3 \
    0.001 \
    0.01
