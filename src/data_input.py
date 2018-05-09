from pyspark.sql.types import StringType

"""
Transfer a list of input path in a list of dataframes
Input:
spark: the main spark session
input_list: a list of input path

Output:
result: a list of data frame
"""
def handle_input(spark, input_list, categorical_dictionary={}):
	result = []
	for i in range(len(input_list)):
		if i in categorical_dictionary:
			result.append(handle_single_input(spark, input_list[i], categorical_dictionary[i]))
		else:
			result.append(handle_single_input(spark, input_list[i]))
	return result


"""
Handle single input path
Input:
spark: the main spark session
path: the path to the input file
categorical_cols: a list of column names that is categorical

Output:
result: a table in form of data frame
"""
def handle_single_input(spark, path, categorical_cols=[]):
	name, filetype = path.split('.')
	if filetype == 'txt':
		table = spark.read.format('txt').options(header='true',inferschema='true').load(path)
	elif filetype == 'json':
		table = spark.read.format('json').options(header='true',inferschema='true').load(path)
	elif filetype == 'csv':
		table = spark.read.format('csv').options(header='true',inferschema='true').load(path)
	else:
		####TO DO can hanld multiple input type
		table = None
	columns = table.columns
	for col in categorical_cols:
		if col in columns:
			table = table.withColumn(col, table[col].cast(StringType()))
	return table

