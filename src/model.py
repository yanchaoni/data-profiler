from pyspark import SparkContext
from data_input import handle_input
from multi_column_algorithm import describe_table
import numpy as np

class DataProfiler():
	"Core System of data profiler project"

	def __init__(self, input_path_list):
		"""
		input_path_list: a list of path of datasets
		"""
		self.spark =  SparkSession.builder.appName("DataProfiler").config("spark.some.config.option", "some-value").getOrCreate()
		self.input_list = input_path_list
		self.num_of_table = len(input_path_list)
		self.tables = handle_input(self.sc, input_path_list)


	def describe(self, indexes=None):
		"""
		indexes: a list of index of table needed to be described
		"""
		if indexes == None:
			indexes = np.arange(self.num_of_table)
		try:
			for i in indexes:
				describe_table(self.tables[i])
		except ValueError:
			print("indexes need to be a list of int")



