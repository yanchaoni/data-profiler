from outlier import outliers
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from model import DataProfiler
# sc = SparkContext('local')
# spark = SparkSession(sc)
# _open = spark.read.format('csv').options(header='true',inferschema='true').load("/user/ecc290/HW1data/open-violations-header.csv")
d = DataProfiler(["/user/ecc290/HW1data/open-violations-header.csv"])
result = outliers(d.tables[0], ["payment_amount", "penalty_amount"], 10, 100, spark=d.spark)
print(result.show())
