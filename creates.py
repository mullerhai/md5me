from pyhive import hive

from pyhive import common
from pyhive import exc
import  pandas as pd
import   numpy as np


host="cdhnode1"
port=10000
user='root'
pwd='******'
database='fk_test'
auth='LDAP'

conn = hive.Connection(host=host, port=port, username=user, auth=auth, password=pwd,
      database=database)
curs=conn.cursor()
hsql="select * from tab_client_label where client_nmbr='AA77' and batch='p3' limit 50"


#hsqlDel="drop  table if exists  tab_testdelete"
#hsqlCrea="""create table  if  not exists  fkdb.y_mid_demo(
# gid int  ,
# realname STRING  ,
# certid STRING
# )
# PARTITIONED BY (client_nmbr  STRING, batch STRING )
# ROW FORMAT DELIMITED  FIELDS  TERMINATED BY '\t' LOCATION 'hdfs://moorecluster/apps/hive/warehouse/fkdb.db/y_mid_demo'"""

#hsqlInsert="insert into table tab_testdelete values(1,'ming'),(2,'fang'),(3,'liang')"
result=curs.execute(hsql)

df=pd.read_sql(hsql,conn)

print(df.head())
#print(result)
