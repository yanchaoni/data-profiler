from pyspark.sql import Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


def per_table_evaluate(tables, func_str, table_indexes, col_names, arg_str=None):
#     print(table_indexes, col_names)
#     print("table index: {0}".format(table_indexes))
#     print( "tables[{0}].".format(table_indexes))
    if isinstance(col_names, str):
        cols ="'" + col_names + "'"
    else:
        cols = "['" + "','".join(col_names) + "']"
    if arg_str is None:
        func = "tables[{0}].".format(table_indexes) + func_str + "(" + cols + ")"
    else:
        func = "tables[{0}].".format(table_indexes) + func_str + "(" + cols + "," + arg_str + ")"
    print(func)
    try:
        result = eval(func)
    except:
        if isinstance(col_names, str):
            dic =  "{'" + col_names + "':'" + func_str + "'}"
        else:
            dic =  "{'" + ("':'"+func_str+"','").join(col_names) + "':'" +  func_str + "'}"
        func = "tables[{0}].".format(table_indexes) + "agg(" + dic + ")"
        print(func)
        result = eval(func)
    return result

def single_column_evaluate(tables, func_str, table_indexes, col_names, arg_str=None):
    profile=[]
    if isinstance(table_indexes, int): # 1 -> 1/many
        profile += [per_table_evaluate(tables, func_str, table_indexes, col_names, arg_str)]
    else: # many -> many
# handle table index not list
# handle function cannot apply on col
# handle col_name not string
        assert isinstance(table_indexes, list), "table_indexes must be either int or list of int"
        assert not isinstance(col_names, str), "Column names for each table are required!"
        assert len(table_indexes) ==  len(col_names), "Number of tables and number of column sets does not match: {0} != {1}".format(len(table_indexes), len(col_names))
        for table_index, col_name in zip(table_indexes, col_names):
            profile += [per_table_evaluate(tables, func_str, table_index, col_name, arg_str)]
    return profile

# def main():
#     sc = SparkContext('local')
#     spark = SparkSession(sc)
#     data1 = sc.parallelize([[1,"a"],[3,"c"]])           # => RDD
#     data1 = data1.map(lambda x: Row(k1 = x[0], k2 = x[1]))
#     table = spark.createDataFrame(data1)
#     tables = []
#     tables.append(table)
#     tables.append(table)
#     results = single_column_evaluate(tables, "avg", [0, 1], ["k1", ["k1", "k2"]], None)
#     for result in results:
#         result.show()

if __name__ == "__main__":
    main()
