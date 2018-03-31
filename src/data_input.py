


def handle_input(sparkcontext, input_list):
	result = []
	for path in input_list:
		result.append(handle_single_input(sparkcontext, path))


def handle_single_input(sparkcontext, path):
	name, filetype = path.split('.')
	if filetype == 'txt':
		table = sparkcontext.textFile(path)
	else:
		####TO DO
		table = None
	return table

