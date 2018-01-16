#!/usr/bin/env python

# -- coding:utf-8 --



import os

import sys

from subprocess import call



from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession


#master = spark://spark:7077
master = os.environ.get("SPARK_MASTER_URL")

spark = SparkSession.builder \

    .master(master) \

    .appName("hive") \

    .enableHiveSupport() \

    .getOrCreate()



TIMESTAMP_COLUMNS = ['created', 'date', 'create', 'time', 'launchDate']





def refresh_model(model):

    df = spark.sql('select * from {model}'.format(model=model))

    df.show()

    first = df.first() or []

    time_columns = filter(lambda key: key in first, TIMESTAMP_COLUMNS)



    partition_column = None



    if time_columns:

        partition_column = time_columns[0]



    if 'id' in first:

        partition_column = 'id'



    if not time_columns:

        return



    spark.sql('drop table if exists {model}'.format(model=model))

    df.repartition(time_columns[0]).write.saveAsTable(model)





def run(filePath):

    filePath = os.path.join(os.getcwd(), filePath)

    executor = None

    if 'postsql' in filePath:

        executor = '/data/spark-2.2.0-bin-hadoop2.7/bin/spark-sql'

    else:

        executor = '/data/apache-hive-2.1.1-bin/bin/hive'



    call("{} -f {}".format(executor,filePath),shell=True)



    model = os.path.splitext(os.path.basename(filePath))[0]

    if executor == 'hive':

        print('model', model)

        refresh_model(model)





if __name__ == '__main__':

    if len(sys.argv) == 2:

        run(sys.argv[1])

    else:

        valid_dirs = ['sql', 'postsql']

        for dir in valid_dirs:

            for dirpath,dirnames,filenames in os.walk(dir):

                for filename in filenames:

                    run(os.path.join(dirpath,filename))
