#table = spark.createDataFrame([('foo','lst',1),('max','lst',1),('lst','dt',2),('foo','show',3)],('k','t','v'))
#table2 = spark.createDataFrame([('foo','max',3),('foo','vs',1),('lst','re',2)],('l','m','n'))
import numpy as np 
import binascii
from csv import reader
import time
from pyspark.sql.types import FloatType, StringType
from pyspark.sql.functions import udf
#the hash function we use would be of the format z = (a*x+b)/c, where num_buk is the number of bucket we assigned to hash,
#a, b is the random number we sample from range(num_buk), with no coincidence, c is the smallest prime number larger than
#the bucket number. we convert string x to 
#col1 comes from table.select(name)

#use joining_path_hash to output column similarities
#to be updated: how to integrate numerical-formed categorical vlaues into analysis

#added min_hash_count
def signature(table,name, a_array, b_array, c_prime):
	hashnum = len(a_array)
	hashmins = np.array([c_prime+1]* hashnum)
	#min hash count from here
	min_hash_count = np.zeros(hashnum)
	for row in table.select(name).rdd.collect():
		try:
			string_hash = binascii.crc32(bytes(row[name],'utf-8')) & 0xffffffff
		except:
			continue
		row_hash = (a_array*string_hash+b_array) % c_prime
		need_change = row_hash < hashmins
		same_thing = row_hash == hashmins
		hashmins[need_change] = row_hash[need_change]
		min_hash_count[need_change] = 1
		min_hash_count[same_thing] += 1
	hashmins = [i.item() for i in hashmins]
	min_hash_count = [int(i.item()) for i in min_hash_count]
	return hashmins, min_hash_count

def single_table_signature(table, table_ind, a_array, b_array, c_prime, spark):
	t_info = table.dtypes
	sig_mat_rows = []
	for col_info in t_info:
		if col_info[1] == 'string':
			name = col_info[0]
			t_c_min, t_c_min_count = signature(table,name,a_array,b_array,c_prime)
			hashnum = len(t_c_min)
			row_zip = zip([table_ind]*hashnum, [name]*hashnum, range(hashnum), t_c_min, t_c_min_count)
			sig_mat_rows.extend([row_val for row_val in row_zip])
	table_cnames = tuple(['table_index','col_name','hash_index','hash_value', 'min_hash_count'])
	return spark.createDataFrame(sig_mat_rows,table_cnames)

def get_hash_coeff(hashnum): 
	a_array = generate_hash_num(hashnum)
	b_array = generate_hash_num(hashnum)
	c_prime = 4294967311
	return a_array, b_array, c_prime

def generate_hash_num(hashnum):
	nums = np.random.choice(2**32,size = hashnum)
	while len(set(nums)) != 100:
		nums = np.array(list(set(nums)))
		other = np.random.choice(2*32,size = 100-len(nums))
		nums = np.concatenate([nums,other])
	return nums

def table_has_categorical(table):
	t_info = table.dtypes
	for t in t_info:
		if t[1] == 'string':
			return True 
	return False

def multiple_table_signature(tables, a_array, b_array, c_prime, table_ind = None, spark):
	if table_ind == None:
		table_ind = range(len(tables))
	has_cat = [table_has_categorical(tables[i]) for i in table_ind]
	table_ind = np.array(table_ind)[np.array(has_cat)]
	table_ind = [i.item() for i in table_ind]
	assert len(table_ind) > 0, "None of the tables provided contains categorical fields."
	all_table_mat = single_table_signature(tables[table_ind[0]],table_ind[0],a_array,b_array,c_prime, spark)
	if len(table_ind) > 1:
		for tind in table_ind[1:]:
			table_mat = single_table_signature(tables[tind],tind,a_array,b_array,c_prime, spark)
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
		jaccard_similarity = round(num_inter/num_uion,2)
	return jaccard_similarity

#write other format to summarize A_contain_B and B_contain_A information
def joining_path_hash(tables,threshold = 0,table_ind = None, hashnum = 100, containing_check = False, spark):
	start = time.time()
	a_array , b_array ,c_prime = get_hash_coeff(hashnum)
	ar = multiple_table_signature(tables,a_array, b_array, c_prime, table_ind, spark)
	print("get the signature !!")
	a1 = ar.selectExpr("table_index as at_ind", "col_name as ac_name", "hash_index as ah_ind", "hash_value as ah_val", "min_hash_count as ah_mincount")
	b1 = ar.selectExpr("table_index as bt_ind", "col_name as bc_name", "hash_index as bh_ind", "hash_value as bh_val", "min_hash_count as bh_mincount")
	a1.createOrReplaceTempView("a1")
	b1.createOrReplaceTempView("b1")
	#how to incorporate the min_hash_count into the join table
	result = spark.sql("select at_ind, ac_name, bt_ind, bc_name, count(*) as counts, mean(after_join) as aj_contribution from \
		(select a1.*, b1.*, ah_mincount*bh_mincount as after_join  from a1 inner join b1 on ah_ind = bh_ind and ah_val = bh_val where at_ind < bt_ind) \
		group by at_ind, bt_ind,ac_name, bc_name order by counts desc")
	result = result.withColumn("similarity",result['counts']/hashnum).drop("counts")
	result = result.filter(result.similarity > threshold)
	end = time.time()
	print(end - start)
	if containing_check:
		A_contain_B = spark.sql("select at_ind, ac_name, bt_ind, bc_name, count(*) as A_contain_B_count from \
			(select * from a1 inner join b1 on ah_ind = bh_ind and ah_val < bh_val where at_ind < bt_ind) \
			group by at_ind, bt_ind,ac_name, bc_name")
		B_contain_A = spark.sql("select at_ind, ac_name, bt_ind, bc_name, count(*) as B_contain_A_count from \
			(select * from a1 inner join b1 on ah_ind = bh_ind and ah_val > bh_val where at_ind < bt_ind) \
			group by at_ind, bt_ind,ac_name, bc_name")
		A_contain_B = A_contain_B.withColumn("A_contain_B_sim",A_contain_B['A_contain_B_count']/hashnum).drop("A_contain_B_count")
		B_contain_A = B_contain_A.withColumn("B_contain_A_sim",B_contain_A['B_contain_A_count']/hashnum).drop("B_contain_A_count")
		inter = result.join(A_contain_B, on = ['at_ind','ac_name','bt_ind','bc_name'], how = 'left_outer')
		result = inter.join(B_contain_A, on = ['at_ind','ac_name','bt_ind','bc_name'], how = 'left_outer')
		result = result.fillna(0)
	return result

#the function that allows the finding of the specific column's best joining candidate
def joining_path_hash_specific(tables,table_ind, col_name, threshold = 0, hashnum = 100, containing_check = False, spark):
	start = time.time()
	assert dict(tables[table_ind].dtypes)[col_name] == 'string', "please select a categorical column to check its joining path."
	a_array , b_array ,c_prime = get_hash_coeff(hashnum)
	col_specific = tables[table_ind].select(col_name)
	ar = single_table_signature(col_specific,0,a_array, b_array, c_prime, spark)
	remove_ind = list(range(len(tables)))
	remove_ind.pop(table_ind)
	br = multiple_table_signature(tables,a_array, b_array, c_prime, remove_ind, spark)
	print("get the signature !!")
	a1 = ar.selectExpr("table_index as at_ind", "col_name as ac_name", "hash_index as ah_ind", "hash_value as ah_val", "min_hash_count as ah_mincount")
	b1 = br.selectExpr("table_index as bt_ind", "col_name as bc_name", "hash_index as bh_ind", "hash_value as bh_val", "min_hash_count as bh_mincount")
	a1.createOrReplaceTempView("a1")
	b1.createOrReplaceTempView("b1")
	#how to incorporate the min_hash_count into the join table
	result = spark.sql("select at_ind, ac_name, bt_ind, bc_name, count(*) as counts, mean(after_join) as aj_contribution from \
		(select a1.*, b1.*, ah_mincount*bh_mincount as after_join  from a1 inner join b1 on ah_ind = bh_ind and ah_val = bh_val) \
		group by at_ind, bt_ind,ac_name, bc_name order by counts desc")
	result = result.withColumn("similarity",result['counts']/hashnum).drop("counts")
	result = result.filter(result.similarity > threshold)
	end = time.time()
	print(end - start)
	if containing_check:
		A_contain_B = spark.sql("select at_ind, ac_name, bt_ind, bc_name, count(*) as A_contain_B_count from \
			(select * from a1 inner join b1 on ah_ind = bh_ind and ah_val < bh_val) \
			group by at_ind, bt_ind,ac_name, bc_name")
		B_contain_A = spark.sql("select at_ind, ac_name, bt_ind, bc_name, count(*) as B_contain_A_count from \
			(select * from a1 inner join b1 on ah_ind = bh_ind and ah_val > bh_val) \
			group by at_ind, bt_ind,ac_name, bc_name")
		A_contain_B = A_contain_B.withColumn("A_contain_B_sim",A_contain_B['A_contain_B_count']/hashnum).drop("A_contain_B_count")
		B_contain_A = B_contain_A.withColumn("B_contain_A_sim",B_contain_A['B_contain_A_count']/hashnum).drop("B_contain_A_count")
		inter = result.join(A_contain_B, on = ['at_ind','ac_name','bt_ind','bc_name'], how = 'left_outer')
		result = inter.join(B_contain_A, on = ['at_ind','ac_name','bt_ind','bc_name'], how = 'left_outer')
		result = result.fillna(0)
	return result

#The function to compute the things and get computed joininig strength
def multi_set_resemble(tables, threshold = 0, table_ind = None, hashnum = 100, containing_check = False, spark):
	result = joining_path_hash(tables,threshold, table_ind,hashnum, containing_check, spark)
	if result.count() != 0:
	#calculate the after_join_size based on acol in table A and bcol in table B
		result = result.withColumn("aj_size",result['similarity']/(1+result['similarity'])*result['aj_contribution'])
		#compute the distinct A and B columns values here, in the future, could be pre-computed and stored somewhere to fasten the multiset resemblence computation.
		distinct_num = {}
		for row in result.rdd.collect():
			t1, n1 = row['at_ind'], row['ac_name']
			t2, n2 = row['bt_ind'], row['bc_name']
			if (t1,n1) not in distinct_num:
				distinct_num[(t1,n1)] = tables[t1].select(n1).distinct().count()
			if (t2,n2) not in distinct_num:
				distinct_num[(t2,n2)] = tables[t2].select(n2).distinct().count()
		calculate_udf = udf(lambda a,b,x,y,t : t*(distinct_num[(a,b)]+distinct_num[(x,y)]), FloatType())
		result = result.withColumn("aj_size",calculate_udf(result.at_ind,result.ac_name,result.bt_ind,result.bc_name,result.aj_size))
		#result = result.withColumn("aj_size", result['aj_size']*(distinct_num[(result['at_ind'],result['ac_name'])] + distinct_num[(result['bt_ind'],result['bc_name'])]))
	return result

#correspondingly give a thing that compute specific multi_set_resemble for a column, change joining_path_hash to joining_path_hash specific
def multi_set_resemble_specific(tables, table_ind, col_name, threshold = 0, hashnum = 100, containing_check = False, spark):
	result = joining_path_hash_specific(tables, table_ind, col_name, threshold, hashnum, containing_check, spark)
	if result.count() != 0:
	#calculate the after_join_size based on acol in table A and bcol in table B
		result = result.withColumn("aj_size",result['similarity']/(1+result['similarity'])*result['aj_contribution'])
		#compute the distinct A and B columns values here, in the future, could be pre-computed and stored somewhere to fasten the multiset resemblence computation.
		distinct_num = {}
		for row in result.rdd.collect():
			t1, n1 = row['at_ind'], row['ac_name']
			t2, n2 = row['bt_ind'], row['bc_name']
			if (t1,n1) not in distinct_num:
				distinct_num[(t1,n1)] = tables[t1].select(n1).distinct().count()
			if (t2,n2) not in distinct_num:
				distinct_num[(t2,n2)] = tables[t2].select(n2).distinct().count()
		calculate_udf = udf(lambda a,b,x,y,t : t*(distinct_num[(a,b)]+distinct_num[(x,y)]), FloatType())
		result = result.withColumn("aj_size",calculate_udf(result.at_ind,result.ac_name,result.bt_ind,result.bc_name,result.aj_size))
		#result = result.withColumn("aj_size", result['aj_size']*(distinct_num[(result['at_ind'],result['ac_name'])] + distinct_num[(result['bt_ind'],result['bc_name'])]))
	return result


def get_table_category(table):
	table_info = table.dtypes
	return [i[0] for i in table_info if i[1] == 'string']

def joining_path_naive(tables, threshold, table_ind = None):
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
	result = result.sort("similarity",ascending = False).filter(result.similarity > threshold)
	end = time.time()
	print(end - start)
	return result


#check if there is any collisions in our hashing functions
# num_distinct = []
# for i in range(100):
# 	hash_set = set()
# 	for row in parking.select('plate_id').rdd.collect():
# 		try:
# 			string_hash = binascii.crc32(bytes(row['plate_id'],'utf-8')) & 0xffffffff
# 		except:
# 			continue
# 		row_hash = (a_array[i]*string_hash+b_array[i])//c_prime
# 		hash_set.add(row_hash)
# 	num_distinct.append(len(hash_set))

# row_hash_num = []
# for row in parking.select('plate_id').rdd.collect():
# 	try:
# 		string_hash = binascii.crc32(bytes(row['plate_id'],'utf-8')) & 0xffffffff
# 	except:
# 		continue
# 	row_hash = (a_array*string_hash+b_array)//c_prime
# 	row_hash_num.append(list(row_hash))
# set_hash_dist = []
# for i in range(100):
# 	hash_set = set()
# 	for item in row_hash_num:
# 		hash_set.add(item)
# 	set_hash_dist.append(len(hash_set))

# def __main__():
# path1 = "/user/ecc290/HW1data/parking-violations-header.csv"
# path2 = "/user/ecc290/HW1data/open-violations-header.csv"
# parking = spark.read.format('csv').options(header='true',inferschema='true').load(path1)
# parking = parking.withColumn("summons_number",parking["summons_number"].cast(StringType()))
# open_vio = spark.read.format('csv').options(header='true',inferschema='true').load(path2)
# open_vio = open_vio.withColumn("summons_number",open_vio["summons_number"].cast(StringType()))
# tables = [parking, open_vio]
# 	join_results = joining_path(tables)
