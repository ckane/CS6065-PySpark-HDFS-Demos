#!/usr/bin/env python
import csv
from pyspark import SparkContext

# Instantiate the SparkContext
sc = SparkContext(appName="PythonCrimeList")

# Load the file from the HDFS
# Second parameter = 1 seems to mean "use HDFS"
# This loads the textfile into an RDD which consists
# of a list of strings, each of which is one line
dataset = sc.textFile('/data/nyc/nyc-traffic.csv', 1)

print("Input set total size: {0}".format(dataset.count()))

# The file will be a CSV where the first row is the
# column headers. Since RDD objects are immutable,
# after we load the data, we need to extract this row
# and then translate the data set into a list of the
# data rows, followed by translating it into the
# keyed data structure.

# Extract the column name row
fnames = dataset.first()
print("First row:")
print(fnames)
print("")

# Create a new RDD that is all the lines, minus the first:
dataset = dataset.filter(lambda x: x != fnames)

# Convert the fieldnames string into a list of ASCII strings
namelist = fnames.encode('utf-8').split(',')

print("Input set data-only size: {0}".format(dataset.count()))
print("First data row (unstructured)")
print(dataset.first())

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


