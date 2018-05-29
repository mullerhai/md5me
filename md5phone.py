import hashlib
import  pandas as  pd
import  numpy as np
from  redis import  Redis
from  pymongo import MongoClient

class Process_md5_phone:
  def __init__(self):
    self.loc_num = list(num for num in range(1, 18))
    self.q_code = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
    self.loc_q_dict = dict(zip(self.loc_num,self.q_code))
    self.v_num = list(num for num in range(0, 11))
    self.v_code = ('1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2')
    self.v_dict = dict(zip(self.v_num,self.v_code))
    self.md5enc=hashlib.md5()
    self.redis_conn=Redis(host='127.0.0.1', port=6379)
    self.mongodB_conn=MongoClient('127.0.0.1', 27017)

#read idcard file  ouput  id list
  def load_phone_file(self,path='data/Mobile.csv'):
    raw=pd.read_csv(path,sep=',',header=None,encoding='utf-8',names=['id',"phone","province","city","cellar","dist_no","post_no"])
    id_prefix_list=list(raw["phone"])
    return id_prefix_list

    # 对 生成的 idcard num 进行加密
  def md5_encode(self, phone):
      m = hashlib.md5()
      # str='130182199005101434'
      m.update(phone.encode())
      res_md5 = m.hexdigest()
      return res_md5



#生成 0000-9999
  def  generate_nums(self):
    num_gs=list("000"+str(num) for num in range(0,10))
    num_ss=list("00"+str(num) for num in  range(10,100))
    num_bs=list("0"+str(num) for num in range(100,1000))
    num_qs=list(str(num) for num in range(100,1000))
    num_gs.extend(num_ss)
    num_gs.extend(num_bs)
    num_gs.extend(num_qs)
    return  num_gs


#生成 批量列表 phone  list
  def generate_phone_all(self, phone_prefix_list,  nums):
    print("Begin generate phone nums !!")
    phone_card_list = list()
    for phone_pre in phone_prefix_list:
          for num in nums:
            new_phone=str(phone_pre)+str(num)
            print(new_phone)
            phone_card_list.append(new_phone)
    return phone_card_list

  def generate_phone(self, phone_prefix_list,  nums,count=100):
    print("Begin generate phone nums !!")
    phone_list = list()
    for phone_pre in phone_prefix_list[:count]:
          for num in nums[:count]:
            new_phone=str(phone_pre)+str(num)
            print(new_phone)
            phone_list.append(new_phone)
    return phone_list

  # 生成  phone md5键 phone 原值 的字典
  def gene_md5_raw_dic(self, phone_list):
      md5_phone_dic = dict()
      for phone in phone_list:
        md5data = self.md5_encode(phone)
        md5_phone_dic[md5data] = phone
        print("md5 : %s , phone : %s"%(md5data,phone))
      return md5_phone_dic

  def  get_md5_dic(self):
    phone_prefix_list=self.load_phone_file()
    nums=self.generate_nums()
    phone_list=self.generate_phone(phone_prefix_list,nums)
    md5_phone_dic=self.gene_md5_raw_dic(phone_list)
    return md5_phone_dic
  # 对生成 idcard num  进行文件追加存储
  def save_file(self,filename,id):
    print("")

  def save_redis(self,md5_rawid_dic):
    print("")
    for  key,value in md5_rawid_dic.items():
      self.redis_conn.set(key,value)

  def save_mongoDB(self,md5_rawid_dic):

    # db = conn.mydb
    # my_set = db.test_set
    print("")


if __name__ == '__main__':
  pp = Process_md5_phone()
  md5_phone_dic=pp.get_md5_dic()
  print("生成的 phone ：" )
  pp.save_redis(md5_phone_dic)
  print("插入redis 成功")


