from pyspark.sql import Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame

SELF_DEFINED_FUNCTION = ["value_count","cat_describe", "distinct_count", "unique_count"]

"""
Input:
	tables: a list of all the tables
	func_str: a string of function name to apply
	table_indexes: the index of the table to apply function
	col_names: a list of column names in this table
	arg_str: extra arguements

Output:
	result: a dataframe of columns with function applied
"""
def per_table_evaluate(tables, func_str, table_indexes, col_names=None, arg_str=None):
	if col_names==None:
		col_names = tables[table_indexes].columns
	if func_str in SELF_DEFINED_FUNCTION:
		func = "{}(tables[{}], {}, {})".format(func_str, table_indexes, col_names, arg_str)
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

	"""
	Input:
		tables: a list of all the tables
		func_str: a string of function name to apply
		col_names: a list of column names, each entry is a list of column names in the respective table
		arg_str: extra arguements

	Output:
		result: a list of dataframe results
	"""
def single_column_evaluate(tables, func_str, table_indexes, col_names=None, arg_str=None):
	profile=[]
	assert isinstance(table_indexes, list), "table_indexes must be list of int!"
	if col_names == None:
		col_names = [None] * len(table_indexes)
	assert isinstance(col_names, list), "col_names must be list of str!"
	assert len(table_indexes) ==  len(col_names), "Number of tables and number of column sets does not match: {0} != {1}".format(len(table_indexes), len(col_names))
	for table_index, per_table_col_names in zip(table_indexes, col_names):
		profile += [per_table_evaluate(tables, func_str, table_index, per_table_col_names, arg_str)]
	return profile



	"""
	Input:
		table: the table to take a look at
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

#override pyspark dataframe describe on categorical variables, mimic pandas style
#summary stats for catgorical variable: count, unique, mode, and mode occurrences
def cat_describe(table,col_names, arg_str = None):
	col_info = table.dtypes
	if col_names == None:
		col_names = table.columns
	cat_names = []
	for di in col_info:
		if di[0] in col_names and di[1] in ['string','binary','boolean']:
			cat_names.append(di[0])
	if len(cat_names) == 0:
		print("cannot perform categorical analysis on the tables and columns provided, please check the column names and types.")
		return

	def single_cat(table,name):
		counts = table.count()
		uniques = table.select(name).distinct().count()
		toprow = table.groupby(name).count().orderBy(['count',name],ascending = [0,0]).first()
		tops, freqs = toprow[name], toprow['count']
		return (counts,uniques,tops,freqs)

	values = [single_cat(table,name) for name in cat_names]
	dt = spark.createDataFrame(values, tuple(['count','uniques','mode','mode_count']))
	return dt

	"""
	Input:
		table: the table to take a look at
		col_names: a list of column name to do distinct_count
		arg_str: extra arguements

	Output:
		result: a list of dataframe of value count result
	"""
def distinct_count(table, col_names, arg_str=None):
    try:
        from pyspark.sql.functions import col, countDistinct
        uniques = table.agg(*(countDistinct(col(c)).alias(c) for c in col_names))
    except:
        print("Cannot resolve column: {}".format(col_names))
    return uniques

	"""
	Input:
		table: the table to take a look at
		col_names: a list of column name to do null_count
		arg_str: extra arguements

	Output:
		result: a list of dataframe of null count result
	"""
def null_count(table, col_names, arg_str=None):
    try:
        nulls = table.select([count(when(isnan(c), c)).alias(c) for c in col_names])
    except:
        print("Cannot resolve column: {}".format(col_names))
    return nulls

def main():
    sc = SparkContext('local')
    spark = SparkSession(sc)
    data1 = sc.parallelize([[1,"a"],[3,"c"]])           # => RDD
    data1 = data1.map(lambda x: Row(k1 = x[0], k2 = x[1]))
    table = spark.createDataFrame(data1)
    tables = []
    tables.append(table)
    tables.append(table)
    results = single_column_evaluate(tables, "distinct_count", [0, 1], None, None)
    print(results)

if __name__ == "__main__":
	main()