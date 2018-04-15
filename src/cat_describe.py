# def cat_describe(table_ind,corr_ind):
# 	table = self.tables[table_ind]
# 	col_info = table.dtypes
# 	colnames = []
# 	for d in corr_ind:
# 		if col_info[d][1] in ['string','binary','boolean']:
# 			colnames.append(col_info[d][0])
# 		counts = tuple([table.count()] * len(colnames))
# 		# s_df = sqlContext.createDataFrame([("foo", 1), ("sar", 2), ("foo", 3)], ('k', 'v'))
# 		uniques = tuple([table.select(name).distinct().count() for name in colnames]) #or distinct.show()
# 		top_rows = [table.groupby(name).count().orderBy(['count',name],ascending = [0,0]).first() for name in colnames]
# 		tops = tuple([top_rows[i][colnames[i]] for i in range(len(colnames))])
# 		freqs = tuple([item['count'] for item in top_rows])
# 	dt = sqlContext.createDataFrame([counts,uniques,tops,freqs], tuple(colnames))
# 	return dt


import numpy as np 
import binascii
#the hash function we use would be of the format z = (a*x+b)/c, where num_buk is the number of bucket we assigned to hash,
#a, b is the random number we sample from range(num_buk), with no coincidence, c is the smallest prime number larger than
#the bucket number. we convert string x to 
#col1 comes from table.select(name)

#use how many hash functions

def signature(table,name, a_array, b_array, c_prime):
	hashnum = len(a_list)
	hashmins = np.array([c_prime+1]* hashnum)
	for row in table.select(name).rdd.collect():
		string_hash = binascii.crc32(bytes(row[name],'utf-8')) & 0xffffffff
		row_hash = (a*string_hash+b)//c_prime
		need_change = row_hash < hashmins
		hashmins[need_change] = row_hash[need_change]
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
	final_mat = spark.createDataFrame(sig_mat_rows,table_cnames)
	return final_mat

def get_hash_coeff(hashnum): 
	a_array = np.random(2**32,size = hashnum)
	b_array = np.random(2**32, size = hashnum)
	c_prime = 4294967311
	return a_array, b_array, c_prime

def multiple_table_signature(tables, table_ind, hashnum = 20):
	a_array , b_array ,c_prime = get_hash_coeff(hashnum)
	if table_ind == None:
		table_ind = range(len(tables))
	all_table_mat = single_table_signature(tables[0],tind,a_array,b_array,c_prime)
	if len(table_ind) > 0:
		for tind in table_ind[1:]:
			table_mat = single_table_signature(tables[tind],tind,a_array,b_array,c_prime)
			all_table_mat = all_table_mat.unionAll(table_mat)
	return all_table_mat

#add the assert value that if tables[t1][cname1] is not string then do something
def get_jaccard_similarity(tables,t1,t2,cname1,cname2, hashnum = 20, option = 'min_hash'):
	if option == 'min_hash':
		a_array , b_array ,c_prime = get_hash_coeff(hashnum)
		s1 = signature(tables[t1],cname1, a_array, b_array, c_prime)
		s2 = signature(tables[t2],cname2, a_array, b_array, c_prime)
		jaccard_similarity = sum(s1 == s2)/hashnum
	if option == 'naive':
		num_inter = (tables[t1].select(cname1)).intersect(tables[t2].select(cname2)).count()
		num_uion = (tables[t1].select(cname1)).unionAll(tables[t2].select(cname2)).distinct().count()
		jaccard_similarity = num_inter/num_uion
	return jaccard_similarity

def joining_path(tables,table_ind, hashnum = 20):
	ar = multiple_table_signature(tables,table_ind,hashnum)
	ar.createOrReplaceTempView("ar")
	result = spark.sql("select a.table_ind, b.table_ind, a.col_name, b.col_name, count(*)/ hashnum as similarity \
		(select * from ar a inner join ar b on a.hash_index = b.hash_index and a.hash_value = b.hash_value \
		where a.table_ind < b.table_ind) br group by a.table_ind, b.table_ind,a.col_name, b.col_name order by similarity desc")
	return result
