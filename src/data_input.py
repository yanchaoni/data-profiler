


def handle_input(spark, input_list):
	result = []
	for path in input_list:
		result.append(handle_single_input(spark, path))


def handle_single_input(sparkcontext, path):
	name, filetype = path.split('.')
	if filetype == 'txt':
		table = spark.read.text(path)
	else:
		####TO DO
		table = None
	return table

