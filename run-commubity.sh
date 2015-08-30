#! /bin/bash

spark-submit --class com.wtist.spark.graphx.ALPAtest.communityDetection \
    --master spark://dell01:7077 \
    --num-executors 10 \
    --driver-memory 20g \
    --executor-memory 20g \
    --executor-cores 8 \
    target/scala-2.10/alpa_2.10-1.0.jar \
    data/wiki-talk-pure.dat \
    0.98 \
    -0.2 \
    0.2 \
    hdfs://dell01:12306/user/cuice/graph/wiki-talk-20150118.dat
