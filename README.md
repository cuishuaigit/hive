# hive
hsql、spark-sql
It's used to executor hsql and spark-sql,in use this script you must install pyspark first!

# pip  install pyspark

you need to add 'export SPARK_MASTER_URL=spark://spark:7077' to .bashrc  file,or in hive.py use spark://spark:7077 rather than  SPARK_MASTER_URL or make this a var in hive.py:

# echo "export SPARK_MASTER_URL=spark://spark:7077" >> .bashrc

the second spark is your hostname,and it read in spark-default.xml,in another hand,you should set ip  hostname in /etc/hosts file so that the process can read it.
