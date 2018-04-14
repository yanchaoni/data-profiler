

"""
Transfer a list of input path in a list of dataframes
Input:
spark: the main spark session
input_list: a list of input path

Output:
result: a list of data frame
"""
def handle_input(spark, input_list):
	result = []
	for path in input_list:
		result.append(handle_single_input(spark, path))
	return result


"""
Handle single input path
Input:
spark: the main spark session
path: the path to the input file

Output:
result: a table in form of data frame
"""
def handle_single_input(spark, path):
	name, filetype = path.split('.')
	if filetype == 'txt':
		table = spark.read.text(path)
	else:
		####TO DO can hanld multiple input type
		table = None
	return table

