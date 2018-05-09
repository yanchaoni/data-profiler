import numpy as np
from math import sqrt
from operator import add
from pyspark.mllib.clustering import KMeans, KMeansModel

def tuple2nparray(x, indices):
    result = np.array([])
    for indice in indices:
        result = np.append(result, x[indice])
    return result

def error(point, clusters):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

def isOutlier(x, clusters, cutoff_distance):
    mindist = error(x, clusters)
    return (mindist > cutoff_distance) * 1

# X: RDD of selected columns
def outliers_RDD(table_rdd, indices, num_clusters, cutoff_distance, maxIterations=10, initializationMode="random", wssse=False):
    X = table_rdd.map(lambda x: tuple2nparray(x, indices))
    clusters = KMeans.train(X, num_clusters, maxIterations=maxIterations, initializationMode=initializationMode)
    if wssse:
        WSSSE = X.map(lambda point: error(point, clusters)).reduce(add)
        print("Within Set Sum of Squared Error, k = " + str(num_clusters) + ": " + str(WSSSE))
    outlier = table_rdd.map(lambda x: tuple(x) + (isOutlier(tuple2nparray(x, indices), clusters, cutoff_distance), ))
    return outlier.filter(lambda x: x[-1]).map(lambda x: x[:-1])

# wssse: True if within set sum of square is needed
def outliers(table, col_names, num_clusters, cutoff_distance, maxIterations=10, initializationMode="random", wssse=False, spark=None):
    columns = table.columns
    indices = []
    for col_name in col_names:
        indices.append(columns.index(col_name))
    result = outliers_RDD(table.rdd, indices, num_clusters, cutoff_distance, maxIterations, initializationMode, wssse)
    result = spark.createDataFrame(result, tuple(columns))
    print("There are {} outliers.".format(result.count()))
    return result

def main():
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    sc = SparkContext('local')
    spark = SparkSession(sc)
    _open = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ecc290/HW1data/open-violations-header.csv")
    result = outliers(_open, ["payment_amount", "penalty_amount"], 10, 100)
    print(result.show())

if __name__ == "__main__":
    main()
