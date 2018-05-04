import numpy as np
from math import sqrt
from operator import add
from pyspark.mllib.clustering import KMeans, KMeansModel

def nparray2tuple(x):
    result = ()
    for xi in x:
        result += (float(xi), )
    return result

def error(point, clusters):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

def isOutlier(x, clusters, cutoff_distance):
    mindist = error(x, clusters)
    return (mindist > cutoff_distance) * 1

# X: RDD of selected columns
def outliers_RDD(X, num_clusters, cutoff_distance, maxIterations=10, initializationMode="random"):
    clusters = KMeans.train(X, num_clusters, maxIterations=maxIterations, initializationMode=initializationMode)
    WSSSE = vso.map(lambda point: error(point, clusters)).reduce(add)
    print("Within Set Sum of Squared Error, k = " + str(num_clusters) + ": " + str(WSSSE))
    outlier = X.map(lambda x: np.append(x, isOutlier(x, clusters, cutoff_distance))).filter(lambda x: x[-1])
    return outlier.map(lambda x: tuple(x[:-1]))

def outliers(table, col_names, num_clusters, cutoff_distance, maxIterations=10, initializationMode="random"):
    X = table[col_names].rdd.map(list).map(np.array)
    result = outliers_RDD(X, num_clusters, cutoff_distance, maxIterations, initializationMode).map(nparray2tuple)
    result = spark.createDataFrame(result, tuple(col_names))
    print("There are {} of outliers.".format(result.count()))
    return spark.createDataFrame(result, tuple(col_names))

