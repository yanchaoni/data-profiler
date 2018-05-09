from pyspark.sql import SparkSession
from data_input import handle_input
from multi_column_algorithm import describe_table
from single_column_algorithm import single_column_evaluate
from cat_describe import multi_set_resemble,joining_path_hash,joining_path_hash_specific,multi_set_resemble_specific
from outlier import outliers
import numpy as np
import pdb
from pyspark.context import SparkContext

class DataProfiler():
	"Core System of data profiler project"

	def __init__(self, input_path_list, categorical_dictionary={}):
		"""
		input:
		input_path_list: a list of path of datasets
		categorical_dictionary: allow user to specify which column is categorical data type,
		this input will take the type of a dictionary, where key is the index of the table, and
		value is a list of column name (string)


		variable:
		self.spark: main sparksession
		self.input_list: input path list
		self.num_of_table: number of tables in the core system
		self.tables: reference to a list of tables as data frame in the core system

		"""
		sc = SparkContext('local')
		self.spark =  SparkSession(sc) # .builder.appName("DataProfiler").config("spark.some.config.option", "some-value").getOrCreate()
		self.input_list = input_path_list
		self.num_of_table = len(input_path_list)
		self.tables = handle_input(self.spark, input_path_list, categorical_dictionary)


	def describe(self, indexes=None):
		"""
		Describe tables in the core system
		input:
		indexes: a list of index of table needed to be described
		"""
		if indexes == None:
			indexes = np.arange(self.num_of_table)
		try:
			for i in indexes:
				describe_table(self.tables[i])
		except ValueError:
			print("indexes need to be a list of int")

	def single_column_evaluation(self, func_str, table_indexes, col_names=None, arg_str=None):
		"""
		Evaluate a single column, numerical and categorical summary
		"""
		return single_column_evaluate(self.tables, func_str, table_indexes, col_names, arg_str)

	def joining_path_hash(self, threshold = 0,table_ind = None, hashnum = 100, containing_check = False):
		return joining_path_hash(self.tables,threshold,table_ind, hashnum, containing_check, self.spark)

	def joining_path_hash_specific(self, table_index, col_name, threshold = 0, hashnum = 100, containing_check = False):
		return joining_path_hash_specific(self.tables,table_index, col_name, threshold, hashnum, containing_check, self.spark)

	def multi_set_resemble(self, threshold = 0.5, table_ind = None, hashnum = 100, containing_check = False):
		"""
		Suggest a join path possibility
		"""
		return multi_set_resemble(self.tables, threshold, table_ind, hashnum, containing_check, self.spark)

	def multi_set_resemble_specific(self, table_ind, col_name, threshold = 0, hashnum = 100, containing_check = False):
		return multi_set_resemble_specific(self.tables, table_ind, col_name, threshold, hashnum, containing_check, self.spark)



	def outliers(self, table_index, col_names, num_clusters, cutoff_distance, maxIterations=10, initializationMode="random", wssse=False):
		"""
		Return records of outliers in specific columns of a table
		"""
		return outliers(self.tables[table_index], col_names, num_clusters, cutoff_distance, maxIterations, initializationMode, wssse, self.spark)

# d = DataProfiler(["/user/ecc290/HW1data/parking-violations-header.csv", "/user/ecc290/HW1data/open-violations-header.csv"],{0:["summons_number"]})
# d.outliers(1, ["payment_amount", "penalty_amount"], 10, 100).show()
# d.multi_set_resemble().show()
# d.joining_path_hash().show()
