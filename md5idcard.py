import hashlib
import  pandas as  pd
import  numpy as np
from  redis import  Redis
from  pymongo import MongoClient

class Process_md5_idcard:
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
  def load_idcard_file(self,path='data/chinese-id-card-area.csv'):
    raw=pd.read_csv(path,sep=',',header=0,encoding='utf-8')
    id_prefix_list=list(raw["id"])
    return id_prefix_list

# 生成 年份 1960-2018
  def  generate_years(self,start=1960,end=2020):
    year=list(yes for yes in range(start,end))
    return year

    # 对 生成的 idcard num 进行加密
  def md5_encode(self, idcard):
      m = hashlib.md5()
      # str='130182199005101434'
      m.update(idcard.encode())
      res_md5 = m.hexdigest()
      return res_md5

#生成 365天
  def generate_days(self,days=366):
    day_index=pd.date_range('2000',periods=days,freq='D')
    rawdays=list(day_index.astype(np.str))
    day_x=list(day[5:7]+day[8:] for day in rawdays)
    return day_x


#生成 000-999
  def  generate_nums(self):
    num_gs=list("00"+str(num) for num in range(0,10))
    num_ss=list("0"+str(num) for num in  range(10,100))
    num_bs=list(str(num) for num in range(100,1000))
    num_gs.extend(num_ss)
    num_gs.extend(num_bs)
    return  num_gs

#拼接字符串 并生成最后一位验证码算法  最终生成的身份证号
  def gene_idstr_vali(self,id_pre,year,day,num):
    id_card = str(id_pre) + str(year) + str(day) + str(num)
    sum = 0
    for index, cha in enumerate(id_card[0:17]):
      sum += int(cha) * self.loc_q_dict[index + 1]
    v_res = sum % 11
    v_last =self.v_dict[v_res]
    newid_card = id_card + v_last
    return newid_card
#生成 批量列表 身份证id  list
  def generate_idnum(self, id_prefix_list, yearlist, days, nums):
    print("Begin generate idnums !!")
    id_card_list = list()
    for id_pre in id_prefix_list:
      for year in yearlist:
        for day in days:
          for num in nums:
            newid_card = self.gene_idstr_vali(id_pre, year, day, num)
            #print(newid_card)
            id_card_list.append(newid_card)
    return id_card_list

  # 生成 最后一位验证码 进行顺序拼接为 idcard num
  def get_valicode(self,pC=10,yC=10,dC=10,nC=10):
    id_prefix_list=self.load_idcard_file()
    yearlist=self.generate_years()
    days=self.generate_days()
    nums=self.generate_nums()
    id_card_list=self.generate_idnum(id_prefix_list[:pC], yearlist[:yC], days[:dC], nums[:nC])
    return id_card_list

  def  get_all_valicode(self):
    id_prefix_list=self.load_idcard_file()
    yearlist=self.generate_years()
    days=self.generate_days()
    nums=self.generate_nums()
    id_card_list=self.generate_idnum(id_prefix_list, yearlist, days, nums)
    return id_card_list
  # 生成  idcardmd5键 idcard 原值 的字典
  def gene_md5_rawid_dic(self, idcard_list):
      md5_rawid_dic = dict()
      for idcard in idcard_list:
        md5data = self.md5_encode(idcard)
        md5_rawid_dic[md5data] = idcard
        print("md5 : %s , id : %s"%(md5data,idcard))
      return md5_rawid_dic

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








  def ks_test(self):
    id_card_list=list()
    id_prefix_list=('110100','110110')
    yearlist=('1990','1991')
    days=('0102','0708')
    nums=('000','333','278')
    loc_num = list(num for num in range(1, 18))
    q_code = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
    q_dict = dict(zip(loc_num, q_code))
    v_num = list(num for num in range(0, 11))
    v_code = ('1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2')
    v_dict = dict(zip(v_num, v_code))
    for id_pre in id_prefix_list:
      for year in  yearlist:
        for day in days:
          for num in nums:
            id_card=id_pre+year+day+num
            sum=0
            for index,cha in enumerate(id_card[0:17]):
              sum+=int(cha)*q_dict[index+1]
            v_res=sum%11
            v_last=v_dict[v_res]
            newid_card=id_card+v_last
            id_card_list.append(newid_card)
            print(newid_card)
    return id_card_list


if __name__ == '__main__':
  dt = Process_md5_idcard()
  #idlist = dt.get_valicode()
  # id_prefix_list = dt.load_idcard_file()
  # yearlist = dt.generate_years()
  # days = dt.generate_days()
  # nums = dt.generate_nums()
  # id_card_list = dt.generate_idnum(id_prefix_list[:1], yearlist[:5], days[:20], nums[:100])

  id_card_list = dt.get_valicode()
  print("生成的 身份证 ：" )

  md5id_dict = dt.gene_md5_rawid_dic(id_card_list)
  print(len(id_card_list))
  dt.save_redis(md5id_dict)
  print("插入redis 成功")


