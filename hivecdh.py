import  pandas as pd
from pyhive import  hive
host="cdhnode1"
port=10000
user='root'
pwd='****'
database='fk_test'
auth='LDAP'

conn = hive.Connection(host=host, port=port, username=user, auth=auth, password=pwd,
      database=database)
curs=conn.cursor()
hsql="select * from tab_client_label where client_nmbr='AA77' and batch='p3' limit 50"

df=pd.read_sql(hsql,conn)

print(df.head())