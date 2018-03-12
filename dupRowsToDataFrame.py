"""
A small script to take an RDD of key-value pairs with
tuples of information and values being the count of duplication
and writes out a distributed dataframe with duplicated and deduplicated
counts of each resource type for each unique ID
"""


if __name__ == '__main__':

    from dataTransformFunctions import *
    import argparse
    from pyspark import SparkContext, SQLContext
    from pyspark.sql.types import *

    # implement argparser
    parser = argparse.ArgumentParser(description='Read and save fileoutputs')
    parser.add_argument('readPath', help = 'path to fhir bundles on hdfs')
    parser.add_argument('writePath', help = 'path to aggregated tuples on hdfs')

    # capture arguments
    args = parser.parse_args()

    # read in the dataset filepath and load data
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    rdd = sc.textFile(args.readPath).map(csvToTuples)

    # apply transformations - create keys, generate values (rows), reduce, flatten
    row_rdd = rdd.map(buildRows).reduceByKey(lambda x, y: [z[0] + z[1] for z in zip(x,y)]).map(flatten)

    # # generate the schema
    columns = ['id', 'con', 'proc', 'obs', 'med', 'con-deDup', 'proc-deDup', 'obs-deDup', 'med-deDup']
    schema = StructType(
        map(lambda x: StructField(x[0], x[1]),
            zip(columns, [StringType()] + [IntegerType()] * 8))
    )

    # generate balanced dataframe
    df = sqlContext.createDataFrame(row_rdd, schema)
    df = df.repartition(500)

    # write out the dataframe
    df.write.parquet(args.writePath)

    row_rdd.map(lambda x: ', '.join(map(str,x))).saveAsTextFile(args.writePath)
