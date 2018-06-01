import redis
import numpy as np
import pandas as pd
from pympler.asizeof import asizeof
from time import time
from string import letters


def _speed_test(input, outside_set, query_indices, name):
	'''tests speed and memory usage of redis set and bloom filter. Looks at speed of appending the value-key pairs from the param
	input. Looks at speed of querying several random keys from input and outside_set parameters. 
	The speed results are printed in addition to the memory used by the set.'''

	r = redis.StrictRedis(host='localhost', port=6379, db=0)
	r.execute_command('FLUSHALL')

	#initializes parameters for redis set and bloom filter based on name param
	if name == "redis":
		add_command, query_command = "SADD", "SISMEMBER"
		print_string = "Redis Set"
	else:
		add_command, query_command = "BF.ADD", "BF.MEXISTS"
		print_string = "Bloom Filter"

	start = time()
	for row in input:
		r.execute_command(add_command, name, row[1])
	insert_time = time()

	for i in query_indices:
		r.execute_command(query_command, name, input[i,1])
		r.execute_command(query_command, name, input[i+199999,1])
		r.execute_command(query_command, name, input[i+399999,1])
		r.execute_command(query_command, name, input[i+599999,1])
		r.execute_command(query_command, name, outside_set[i,1])
	query_time = time()
	mem = r.execute_command('MEMORY USAGE', name)

	print("{:^18} {:^16} {:^16} {:^10} {:>12}".format(print_string, insert_time-start, query_time-insert_time, asizeof(r), mem))
	r.execute_command('FLUSHALL')



def speed_comparison():
	'''uses function above to compare the speed and memory usage of bloom filter and redis set. Uses 80% of majestic million at a time so that data that is not
	stored in the database can be queried'''

	#creates array of url and rank pairs
	mm_arr = np.array(pd.read_csv('majestic_million.csv',header=None, skiprows=1, usecols=[2,0]))
	mm_arr = np.array_split(mm_arr, 5)

	print("\n{:^18} {:^16} {:^16} {:^10} {:^16}".format("Data Structure", "Insertion Time", "Query Time", "# Bytes", "Memory Usage"))
	for i in range(5):
		#set of random indices to query the data structures
		rand_inds = np.random.randint(200000, size=(10,1))
		
		#you can't concatenate arrays with numpy unless the dimension is the same
		before_i = mm_arr[0:i:1]
		after_i = mm_arr[i+1:5:1]
		if i == 0: 
			input_sets = np.concatenate(after_i)
		elif i == 4:
			input_sets = np.concatenate(before_i)
		else:
			input_sets = np.concatenate(np.concatenate((before_i,after_i),axis=0))

		_speed_test(input_sets, mm_arr[i], rand_inds,"redis")
		_speed_test(input_sets, mm_arr[i], rand_inds,"bloom")



def _random_string_gen(n):
	'''helper function of error_comparison_suite, it returns a list of n randoml generated strings, each with length 24.'''
	np.random.seed(1)
	print("Generating random strings...")

	chars = [c.encode('ascii') for c in letters]
	# strings = [''.join([chars[np.random.random_integers(0,51)] for x in range(24)]) for y in range(n)]
	strings = [''.join([chars[int(np.random.random()*52)] for x in range(24)]) for y in range(n)]
	return strings



def _error_comparison_test(name, mm, strings, n):
	'''creates a redis database using a data structure from the parameter name. Prints the error rates, collisions, time, and memory usage when adding
	the majestic million (mm) and n randomly generated strings to the data structure. Since the bloom filter can have different error rates, this function 
	iterates over the possible error rates and prints the corresponding results.'''

	r = redis.StrictRedis(host='localhost', port=6379, db=2)
	r.execute_command('FLUSHALL')

	#initializes variables based on the data structure given in name param
	if name == "redis": 
		print_string, add_command = "\nRedis Set with "+str(n+1000000)+" input strings", "SADD"
		error = [0]		
	elif name == "bloom":
		print_string, add_command = "\nBloom Filter with "+str(n+1000000)+" input strings", "BF.ADD"
		error =  [0.01,0.001,0.0001,0.00001,0.000001] 	#<-- Note that a false positive rate of 1% == 0.01 error rate
	elif name == "cuckoo":
		print_string, add_command = "\nCuckoo Filter with "+str(n+1000000)+" input strings", "CF.ADDNX"
		error =  ["~0.03"]
		r.execute_command('CF.RESERVE', 'cuckoo0', 1000000+n) #<-- reserves space to reduce error for cuckoo
	else:
		raise ValueError("Invalid data structure name")

	print(print_string+"\n{:^12} {:^16} {:^12}   {:>12}".format("Error Rate", "# of Collisions", "Time", "Memory Usage"))

	for i in range(len(error)):
		if name == "bloom": r.execute_command('BF.RESERVE', name+str(i), error[i], 1000000+n)

		count = 0
		start = time()
		for row in mm:		#adds majestic million
			count += r.execute_command(add_command, name+str(i), row[1])

		for string in strings:	#adds randomly generated strings
			count += r.execute_command(add_command, name+str(i), string)
		end = time()
		mem = r.execute_command('MEMORY USAGE', name+str(i))

		print("{:^12} {:^16} {:^12}  {:>12}".format(error[i], 1000000+n-count, end-start, mem))
		r.execute_command('FLUSHALL')



def error_comparison_suite(n=0, redis_set=False, bloom=False, cuckoo=False):
	'''automates process of running the error comparison function above for different data structures. Allows multiple tests to be ran using the same set of generated strings.
	The results of the test are printed via the individual test functions.

	Parameters:
	n (int): number of strings to be generated for the tests to be used in addition to majestic million, defaults to 0
	redis_set (bool): runs the error_comparison test for the redis_set
	bloom (bool): runs the error comparison test for the bloom filter, note that this test is repeated with different error rates
	cuckoo (bool): runs the errr comparison test for the cuckoo filter'''

	if redis_set or bloom or cuckoo:

		#creates array of url and rank pairs
		mm_arr = np.array(pd.read_csv('majestic_million.csv',header=None, skiprows=1, usecols=[2,0]))

		strings = _random_string_gen(n)

		if redis_set: _error_comparison_test("redis", mm_arr, strings, n)
		if bloom: _error_comparison_test("bloom", mm_arr, strings, n)
		if cuckoo: _error_comparison_test("cuckoo", mm_arr, strings, n)

	else:
		raise ValueError("At least one parameter must be true to perform a test")



#----MAIN-------------------


# error_comparison_suite(10000000,True,True,True)
# speed_comparison()