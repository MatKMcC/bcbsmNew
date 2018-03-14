"""
A small script to get json data into balanced and dense datasets
for further EDA into causes of duplication
"""



if __name__ == '__main__':

    from dataPrepFunctions import *
    from pyspark import SparkContext
    import json
    import argparse

    sc = SparkContext()

    # implement argparser
    parser = argparse.ArgumentParser(description='Read and save fileoutputs')
    parser.add_argument('readPath', help = 'path to fhir bundles on hdfs')
    parser.add_argument('writePath', help = 'path to aggregated tuples on hdfs')

    # capture arguments
    args = parser.parse_args()

    # read in the dataset filepath and load data
    bundles = sc.textFile(args.readPath).map(json.loads)

    # extract tuples of pertinent information - (ID, resourceType, date, sys, code)
    tupes = bundles.flatMap(generateTuples).repartition(1000) # is this helpful???????

    # get count of tuple level duplication
    tupes_counted = tupes.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

    # the following two could be simplified by saving as a pickle object?

    # flatten the results - for csv
    tupes_counted = tupes_counted.map(lambda x: tuple(flatten(x)))

    # I assume that spark automatically optimized the code above and here?
    tupes_counted = tupes_counted.map(lambda x: ', '.join(map(str,x)))

    # repartition and write out balanced RDDs
    tupes_counted.repartition(1000).saveAsTextFile(args.writePath)
