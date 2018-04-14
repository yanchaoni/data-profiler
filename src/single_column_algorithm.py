from pyspark.sql import Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame

SELF_DEFINED_FUNCTION = ["value_count"]


def per_table_evaluate(tables, func_str, table_indexes, col_names=None, arg_str=None):
#     print(table_indexes, col_names)
#     print("table index: {0}".format(table_indexes))
#     print( "tables[{0}].".format(table_indexes))
    if col_names==None:
        col_names = tables[table_indexes].columns
    if func_str in SELF_DEFINED_FUNCTION:
        func = "{}({}, {}, {})".format(func_str, tables[table_indexes], col_names, arg_str)
        print(func)
        return eval(func)
    if arg_str is None:
        func = "tables[{}].{}({})".format(table_indexes, func_str, col_names)
    else:
        func = "tables[{}].{}({}, {})".format(table_indexes, func_str, col_names, arg_str)
    print(func)
    try:
        result = eval(func)
    except:
        dic =  "{'" + ("':'"+func_str+"','").join(col_names) + "':'" +  func_str + "'}"
        func = "tables[{0}].".format(table_indexes) + "agg(" + dic + ")"
        print(func)
        result = eval(func)
    return result

def single_column_evaluate(tables, func_str, table_indexes, col_names=None, arg_str=None):
    profile=[]
# handle col_name not string
    assert isinstance(table_indexes, list), "table_indexes must be list of int!"
    assert isinstance(col_names, list), "col_names must be list of str!"
    assert len(table_indexes) ==  len(col_names), "Number of tables and number of column sets does not match: {0} != {1}".format(len(table_indexes), len(col_names))
    for table_index, per_table_col_names in zip(table_indexes, col_names):
        profile += [per_table_evaluate(tables, func_str, table_index, per_table_col_names, arg_str)]
    return profile



    """
    Input:
        table: the table to take a take a look at
        col_names: a list of column name to do value count
        arg_str: extra arguements

    Output:
        result: a list of dataframe of value count result
    """
def value_count(table, col_names, arg_str=None):
    result = []
    for col in col_names:
        try:
            result.append(table.groupby(col).count())
        except:
            print("Cannot resolve column: {}".format(col))
            continue
    return result


def main():
    sc = SparkContext('local')
    spark = SparkSession(sc)
    data1 = sc.parallelize([[1,"a"],[3,"c"]])           # => RDD
    data1 = data1.map(lambda x: Row(k1 = x[0], k2 = x[1]))
    table = spark.createDataFrame(data1)
    tables = []
    tables.append(table)
    tables.append(table)
    results = single_column_evaluate(tables, "value_count", [0, 1], [["k1"], ["k1", "k2"]], None)
    for result in results:
        result.show()

if __name__ == "__main__":
    main()
