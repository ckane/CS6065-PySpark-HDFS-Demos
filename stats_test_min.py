#!/usr/bin/env python
import csv
from pyspark import SparkContext

# Instantiate the SparkContext
sc = SparkContext(appName="PythonCrimeList")

dataset = sc.textFile('/data/nyc/nyc-traffic.csv', 1)

# Extract the column name row
fnames = dataset.first()

# Create a new RDD that is all the lines, minus the first:
dataset = dataset.filter(lambda x: x != fnames)

# Convert the fieldnames string into a list of ASCII strings
namelist = fnames.encode('utf-8').split(',')

# Use the non-Spark csv module to translate each partition of
# unstructured data into structured Python dict-compatible keyed
# structures
dataset = dataset.mapPartitions(lambda x: csv.DictReader(x, namelist))

# Display the first row of the data set, showing that it's translated into
# structured data
print("First data row (structured):")
print(dataset.first())
print("Total count: {0}".format(dataset.count()))
print("")


