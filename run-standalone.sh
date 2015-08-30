#! /bin/bash

spark-submit --class com.wtist.spark.graphx.ALPAtest.ALPA \
    --master spark://dell01:7077 \
    --num-executors 8 \
    --driver-memory 10g \
    --executor-memory 20g \
    --executor-cores 8 \
    target/scala-2.10/alpa_2.10-1.0.jar \
    0.95 \
    -0.2 \
    0.2 \
    hdfs://dell01:12306/user/cuice/graph/result
