#table = spark.createDataFrame([('foo','lst',1),('max','lst',1),('lst','dt',2),('foo','show',3)],('k','t','v'))
#table2 = spark.createDataFrame([('foo','max',3),('foo','vs',1),('lst','re',2)],('l','m','n'))
import numpy as np 
import binascii
from csv import reader
import time
#the hash function we use would be of the format z = (a*x+b)/c, where num_buk is the number of bucket we assigned to hash,
#a, b is the random number we sample from range(num_buk), with no coincidence, c is the smallest prime number larger than
#the bucket number. we convert string x to 
#col1 comes from table.select(name)

#use how many hash functions

def signature(table,name, a_array, b_array, c_prime):
	hashnum = len(a_array)
	hashmins = np.array([c_prime+1]* hashnum)
	for row in table.select(name).rdd.collect():
		try:
			string_hash = binascii.crc32(bytes(row[name],'utf-8')) & 0xffffffff
		except:
			continue
		row_hash = (a_array*string_hash+b_array)//c_prime
		need_change = row_hash < hashmins
		hashmins[need_change] = row_hash[need_change]
	hashmins = [i.item() for i in hashmins]
	return hashmins

def single_table_signature(table, table_ind, a_array, b_array, c_prime):
	t_info = table.dtypes
	sig_mat_rows = []
	for col_info in t_info:
		if col_info[1] == 'string':
			name = col_info[0]
			t_c_min = signature(table,name,a_array,b_array,c_prime)
			hashnum = len(t_c_min)
			row_zip = zip([table_ind]*hashnum, [name]*hashnum, range(hashnum), t_c_min)
			sig_mat_rows.extend([row_val for row_val in row_zip])
	table_cnames = tuple(['table_index','col_name','hash_index','hash_value'])
	return spark.createDataFrame(sig_mat_rows,table_cnames)

def get_hash_coeff(hashnum): 
	a_array = np.random.randint(2**32,size = hashnum)
	b_array = np.random.randint(2**32, size = hashnum)
	c_prime = 4294967311
	return a_array, b_array, c_prime


def table_has_categorical(table):
	t_info = table.dtypes
	for t in t_info:
		if t[1] == 'string':
			return True 
	return False

def multiple_table_signature(tables, table_ind = None, hashnum = 100):
	a_array , b_array ,c_prime = get_hash_coeff(hashnum)
	if table_ind == None:
		table_ind = range(len(tables))
	has_cat = [table_has_categorical(tables[i]) for i in table_ind]
	table_ind = np.array(table_ind)[np.array(has_cat)]
	table_ind = [i.item() for i in table_ind]
	assert len(table_ind) > 0, "None of the tables provided contains categorical fields."
	all_table_mat = single_table_signature(tables[table_ind[0]],table_ind[0],a_array,b_array,c_prime)
	if len(table_ind) > 1:
		for tind in table_ind[1:]:
			table_mat = single_table_signature(tables[tind],tind,a_array,b_array,c_prime)
			all_table_mat = all_table_mat.unionAll(table_mat)
	return all_table_mat

#add the assert value that if tables[t1][cname1] is not string then do something
def get_jaccard_similarity(tables,t1,t2,cname1,cname2, hashnum = 100, option = 'min_hash'):
	if option == 'min_hash':
		a_array , b_array ,c_prime = get_hash_coeff(hashnum)
		s1 = np.array(signature(tables[t1],cname1, a_array, b_array, c_prime))
		s2 = np.array(signature(tables[t2],cname2, a_array, b_array, c_prime))
		jaccard_similarity = np.sum(s1==s2)/hashnum
	if option == 'naive':
		num_inter = (tables[t1].select(cname1)).intersect(tables[t2].select(cname2)).count()
		num_uion = (tables[t1].select(cname1)).unionAll(tables[t2].select(cname2)).distinct().count()
		jaccard_similarity = num_inter/num_uion
	return jaccard_similarity

def joining_path_hash(tables,table_ind = None, hashnum = 100):
	start = time.time()
	ar = multiple_table_signature(tables,table_ind,hashnum)
	print("get the signature !!")
	a1 = ar.selectExpr("table_index as at_ind", "col_name as ac_name", "hash_index as ah_ind", "hash_value as ah_val")
	b1 = ar.selectExpr("table_index as bt_ind", "col_name as bc_name", "hash_index as bh_ind", "hash_value as bh_val")
	a1.createOrReplaceTempView("a1")
	b1.createOrReplaceTempView("b1")
	result = spark.sql("select at_ind, ac_name, bt_ind, bc_name, count(*) as counts from \
		(select * from a1 inner join b1 on ah_ind = bh_ind and ah_val = bh_val where at_ind < bt_ind) \
		group by at_ind, bt_ind,ac_name, bc_name order by counts desc")
	result = result.withColumn("similarity",result['counts']/hashnum).drop("counts")
	end = time.time()
	print(end - start)
	return result


def get_table_category(table):
	table_info = table.dtypes
	return [i[0] for i in table_info if i[1] == 'string']

def joining_path_naive(tables, table_ind = None):
	start = time.time()
	if table_ind == None:
		table_ind = range(len(tables))
	has_cat = [table_has_categorical(tables[i]) for i in table_ind]
	table_ind = np.array(table_ind)[np.array(has_cat)]
	table_ind = [i.item() for i in table_ind]
	numtable = len(table_ind)
	js_rows = []
	for aind in range(numtable):
		at_ind = table_ind[aind]
		anames = get_table_category(tables[at_ind])
		for ac_name in anames:
			for bind in range(aind+1,numtable):
				bt_ind = table_ind[bind]
				bnames = get_table_category(tables[bt_ind])
				for bc_name in bnames:
					naive_js = get_jaccard_similarity(tables,at_ind,bt_ind,ac_name,bc_name, option = 'naive')
					if naive_js != 0:
						js_rows.append((at_ind,ac_name,bt_ind,bc_name,naive_js))
	result = spark.createDataFrame(js_rows,tuple(['at_ind','bt_ind','ac_name','bc_name','similarity']))
	end = time.time()
	print(end - start)
	return result





# def __main__():
# 	path1 = "/user/ecc290/HW1data/parking-violations-header.csv"
# 	path2 = "/user/ecc290/HW1data/open-violations-header.csv"
# 	parking = spark.read.format('csv').options(header='true',inferschema='true').load(path1)
# 	open_vio = spark.read.format('csv').options(header='true',inferschema='true').load(path2)
# 	tables = [parking, open_vio]
# 	join_results = joining_path(tables)
