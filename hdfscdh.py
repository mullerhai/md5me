
import os
import platform
import logging as lg
from hdfs.client import Client
from hdfs.client import _logger


host="﻿cdhnode1"
port=50070
user='root'
pwd='******'
database='fk_test'
hpath="/originData/clientlabel/AA77p2_20180525.txt"
auth='LDAP'

from hdfs.client import Client
client = Client("http://10.111.32.18:50070/")  # 50070: Hadoop默认namenode
re=client.list("/originData")
print(re)