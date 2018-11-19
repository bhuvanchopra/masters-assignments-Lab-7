import sys, os, gzip, re, uuid
import datetime as dt
from pyspark.sql import SparkSession, functions, types
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') \
.config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
def parse(line):
    if line_re.match(line):
        return (str(uuid.uuid1()), line_re.match(line).group(1), dt.datetime.strptime(line_re.match(line).group(2), '%d/%b/%Y:%H:%M:%S'), line_re.match(line).group(3), int(line_re.match(line).group(4)))
    else:
        return (None, None, None, None, None)

def main(input_dir, keyspace, table):

    data = spark.sparkContext.textFile(input_dir)
    request = data.map(parse)
    df_schema = types.StructType([
    types.StructField('id', types.StringType(), True),
    types.StructField('host', types.StringType(), True),
    types.StructField('datetime', types.DateType(), True),
    types.StructField('path', types.StringType(), True),
    types.StructField('bytes', types.IntegerType(), True)])
    
    df = spark.createDataFrame(request, df_schema).dropna().repartition('host')

    df.write.format("org.apache.spark.sql.cassandra").mode('overwrite').option('confirm.truncate', True) \
    .options(table=table, keyspace=keyspace).save()

if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(input_dir, keyspace, table)
