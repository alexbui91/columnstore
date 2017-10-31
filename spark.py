from subprocess import Popen,PIPE,STDOUT,call
from pyspark.sql.types import *
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf
# from pyspark.sql import SQLContext
import time
import argparse


spark, sc = None, None
schema_entities = {}

q1 = "select * from events join sensors on events.sid = sensors.sid"

q1_total = "select count(*) from events join sensors on events.sid = sensors.sid"

q2 = "select * from events join sensors on events.sid = sensors.sid where sensors.type = 1 and events.v < 1000000"

q2_total = "select count(*) from events join sensors on events.sid = sensors.sid where sensors.type = 1 and events.v < 1000000"

q3 = 'select * from events join sensors on events.sid = sensors.sid join entities on entities.eid = sensors.eid where entities.name = "Ball 1" and events.v < 1000000'

q3_total = 'select count(*) from events join sensors on events.sid = sensors.sid join entities on entities.eid = sensors.eid where entities.name = "Ball 1" and events.v < 1000000'
# q4 = "select count(events.sid) from events join sensors on events.sid = sensors.sid where sensors.type = 2 and events.v > 5000000"

queries = {'q1': q1, 'q2': q2, 'q3': q3, 'q1_total': q1_total, 'q2_total': q2_total, 'q3_total': q3_total}


def init_table(baseurl):
    date_format = '%Y-%m-%d'
    events, sensors, entities = None, None, None
    events = sc.textFile("%s/sample-game.csv" % baseurl) \
                .map(lambda row: row.split(',')) \
                .map(lambda row: [int(i) for i in row])
    sensors = sc.textFile("%s/sensors.csv" % baseurl) \
                .map(lambda row: row.split(',')) \
                .map(lambda row: [int(i) for i in row])
    entities = sc.textFile("%s/entities.csv" % baseurl) \
                .map(lambda row: row.split(',')) \
                .map(lambda row: [int(row[0])] + [row[1].replace("\"", "")] + [int(row[2])])

    return events, sensors, entities


def init_spark():
    global spark, sc, schema_entities
    conf = (SparkConf().setAppName("assignment"))
    # conf.set("spark.driver.memory", "256g")
    # conf.set("spark.executor.memory", "128g")
    # conf.set("spark.ui.port", p.port)
    # conf.set("spark.sql.shuffle.partitions", "100")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    events, sensors, entities = init_table("raw")

    s_events = StructType([
        StructField("sid", IntegerType(), True),
        StructField("ts", LongType(), True),
        StructField("x", IntegerType(), True),
        StructField("y", IntegerType(), True),
        StructField("z", IntegerType(), True),
        StructField("v", IntegerType(), True),
        StructField("a", IntegerType(), True),
        StructField("vx", IntegerType(), True),
        StructField("vy", IntegerType(), True),
        StructField("vz", IntegerType(), True),
        StructField("ax", IntegerType(), True),
        StructField("ay", IntegerType(), True),
        StructField("az", IntegerType(), True)])

    s_entities = StructType([
        StructField("eid", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("type", IntegerType(), True)])

    s_sensors = StructType([
        StructField("sid", IntegerType(), True),
        StructField("eid", IntegerType(), True),
        StructField("type", IntegerType(), True)])

    schema_entities = {
        "events" : (events, s_events),
        "sensors" : (sensors, s_sensors),
        "entities" : (entities, s_entities)
    }


def get_memory(name):
    now = time.time()
    proc=Popen('free -m', shell=True, stdout=PIPE, )
    output=proc.communicate()[0]
    with open("%s.txt" % name, 'a') as file:
        file.write(('%f' % now) + '\n' + output)


def init_temple_dataframe():
    for key, value in schema_entities.iteritems():
        if value[0]:
            df = spark.createDataFrame(value[0], value[1])
            df.registerTempTable(key)


def main(qname, query):
    after = time.time()
    result = spark.sql(query)
    get_memory(qname)
    result.show()
    get_memory(qname)
    end = time.time()
    running_time = end - after
    print("Time to run query %s is: %is" % (qname, running_time))
    get_memory(qname)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query")
    parser.add_argument("-a", "--all", type=int, default=0)

    args = parser.parse_args()
    get_memory("load_spark")
    init_spark()
    get_memory("load_spark")
    init_temple_dataframe()
    get_memory("load_spark")

    a = ['q1', 'q2', 'q3']
    for x in a:
        main(x, queries[x])
        main('Total records of %s' % x, queries[x + '_total'])
    # a = ["Ball 1","Ball 2","Ball 3","Ball 4","Nick Gertje","Dennis Dotterweich",
    #     "Niklas Waelzlein","Wili Sommer","Philipp Harlass","Roman Hartleb","Erik Engelhardt",
    #     "Sandro Schneider","Leon Krapf","Kevin Baer","Luca Ziegler","Ben Mueller","Vale Reitstetter",
    #     "Christopher Lee","Leon Heinze","Leo Langhans","Referee"]
    # for x in a:
    #     q3 = 'select * from events join sensors on events.sid = sensors.sid join entities on entities.eid = sensors.eid where entities.name = "%s" and events.v > 5000000' % x
    #     print("name", x)
    #     main("q3", q3)   
    # main("q4", q4)   