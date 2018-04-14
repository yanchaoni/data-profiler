from pyspark.sql import Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

def per_table_evaluate(tables, func_str, table_indexes, col_names, arg_str=None):
    if isinstance(col_names, str):
        col_names="'" + col_names + "'"
    else:
        col_names = "['" + "','".join(col_names) + "']"
    if arg_str is None:
        print(table_indexes, col_names)
        print("table index: {0}".format(table_indexes))
        print( "tables[{0}].".format(table_indexes))
        func = "tables[{0}].".format(table_indexes) + func_str + "(" + col_names + ")"
    else:
        func = "tables[{0}].".format(table_indexes) + func_str + "(" + col_names + "," + rg_str + ")"
    print(func)
    return eval(func)

def single_column_evaluate(tables, func_str, table_indexes, col_names, arg_str=None):
    profile=[]
    if isinstance(table_indexes, int): # 1 -> 1/many
        profile += [per_table_evaluate(tables, func_str, table_indexes, col_names, arg_str)]
    else: # many -> many
        assert not isinstance(col_names, str), "Column names for each table are required!"
        assert len(table_indexes) ==  len(col_names), "Number of tables and number of column sets does not match: {0} != {1}".format(len(table_indexes), len(col_names))
        for table_index, col_name in zip(table_indexes, col_names):
            profile += [per_table_evaluate(tables, func_str, table_index, col_name, arg_str)]
    return profile

def main():
        data1 = sc.parallelize([[1,2],[3,4]])           # => RDD
        data1 = data1.map(lambda x: Row(k1 = x[0], k2 = x[1]))
        table = spark.createDataFrame(data1)
        tables = []
        tables.append(table)
        print(single_column_evaluate(tables, "describe", 0, ["k1", "k2"], None))

if __name__ == "__main__":
    main()
