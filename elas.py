#!/usr/bin/python3
import os
import re
import hashlib
import json
import sys
import requests
import time
import subprocess
import socket
from elasticsearch import Elasticsearch

def put_file(dir_path):

	# I dislike using system calls however I don't want to mess with other modules now.
	# This is gonna be ugly - if not already.
	# #This takes into account that the private key used to connect is already cached.
	# the p.wait shoud return a - when  the process is completed.

	rd = "/home/logger/images/"
	proc = subprocess.Popen(["scp", dir_path, "logger@localhost:" + rd])
	while True:

		if not proc.wait():
			break
		else:
			time.sleep(.1)


def make_me_a_hash(path, block_size=2 ** 20):
	sha1 = hashlib.sha1()
	with open(path, "rb") as f:
		while True:
			buf = f.read(block_size)
			if not buf:
				break
			sha1.update(buf)
	return sha1.hexdigest()


def make_simple_json_record(file, digest, path, dt, size, host_name):
	a = {
		"digest": digest,
		"date_time": dt,
		"file_size": size,
		"file_name": file,
		"dir_path": path,
		"orig_host": host_name,
		"reference ": "http://localhost/images/" + file}
	return json.dumps(a)


def make_date(d):
	# mmm,  should this be a def since it's only one line.
	return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(d.split('.')[0])))


def get_size(sz):
	return os.path.getsize(sz)


def get_dir():
	# - Kinda long but effective
	# use default or get input user:

	default_dir = str("/opt/ecel")
	return_dir = str()
	cnt = int(0)

	# test the default dir - could be wrong:
	if not os.path.isdir(default_dir):
		print("The hardcoded default dir ", default_dir, " does not exist. Can't continue")
		sys.exit(1)

	while not return_dir:

		user_dir = input("Enter main directory to parse [" + default_dir + "]")
		if cnt >= 3:
			print("please check the proper location of your directory.")
			sys.exit(1)
		if not user_dir:
			return_dir = default_dir
			break
		elif user_dir and os.path.isdir(user_dir):  # check if read dir is something and that it exists
			return_dir = user_dir
			break
		elif user_dir and not os.path.isdir(user_dir):
			print(user_dir, " does not exists.")
			user_dir = ""
		cnt += 1
	return return_dir


def get_assmt_name():
	ret_value = str()

	while True:
		ret_value = input("enter an assessment name: ")
		if ret_value:
			break

	return ret_value

def chk_cached_key():
	return_value = 0
	cmd = 'ssh-add -l'
	res = subprocess.check_output(cmd, shell=True)
	if re.search("logger", res.decode('utf-8')):
		return_value = 1

	return return_value



def main():

	if not chk_cached_key(): #check for the cashed key
		sys.exit()

	index = get_assmt_name()
	doc_type = str("png_loc")
	es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
	response = requests.get('http://localhost:9200')
	i = 1
	if response.status_code == 200:

		dir_to_read = get_dir()
		if re.search(":$", dir_to_read):
			dir_to_read = dir_to_read + "\\"

		# parse directories and sub-dirs
		for root, dirs, files in os.walk(dir_to_read):

			for name in files:
				path = (os.path.join(root, name))
				# pattern to match the file name
				if re.search("^\d*\.\d{2}", name) and re.search("png$", name, re.IGNORECASE):
					print("working ", path)

					# fix to other m-digest -
					md5 = make_me_a_hash(path)

					date_time = make_date(name)
					f_size = get_size(path)
					# make the json type format for insertion into elastic.

					insert_data = make_simple_json_record(name, md5, path, date_time, f_size, socket.gethostname())
					put_file(path)

					try:  # to insert the record
						# es.index(index=index, doc_type=doc_type, id=i, body=insert_data)
						es.index(index=index, doc_type=doc_type, body=insert_data)
						i += 1
					except:  # bail if something happens.
						print("something happened")
						sys.exit(1)

	else:
		print("not connecting to port 9200")
		sys.exit(1)


if __name__ == '__main__':
	main()
