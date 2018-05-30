import pandas  as pd
import  numpy  as  np

import  hashlib
import os

class ETL_Label:
  #初始化 会读取文件 ，并对文件做最初步的清洗
  def __init__(self,file_path,raw_header_loc_char_dict,sense_code,date_filed,phone_filed,sheet_name='Sheet1',client_nmbr="AA00",batch="p0",header=0,encoding='gbk',raw_header_loc=()):
    #标准的 入库 列名 顺序
    self.std_filed_list=("gid","realname","certid","mobile","card","apply_time","y_label","apply_amount","apply_period","overdus_day","sense_code")
    col_index=(f  for f in range(0,11))
    #标准的 入库 列名加索引字典
    self.std_filed_dict=dict(zip(col_index,self.std_filed_list))
    #excel sheet表格 列单元 索引顺序对应的默认字母
    self.sheet_char_index=('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T')
    #标准的 入库 列名 与 excel sheet表格 列单元 索引顺序对应的默认字母 的对应字典，不存在用 * 星号 表示
    self.raw_header_loc_char_dict=raw_header_loc_char_dict
    #读取 要处理的文件路径 相对路径
    self.file_path=file_path
    #excel 表格 的样本所在的 sheet 名称
    self.sheet_name=sheet_name
    self.client_nmbr=client_nmbr
    self.batch=batch
    self.header=header
    self.encoding=encoding
    self.sense_code=sense_code
    #标准列名 apply_time对应的原始 excel 表格 业务列名
    self.date_filed=date_filed
    #标准列名 mobile  对应的原始 excel 表格 业务列名
    self.phone_filed=phone_filed
    # 实际 清洗excel 文件 原始列名 与标准的 入库 列名 索引的对应字典
    self.raw_header_loc = raw_header_loc
    #获取当前执行路径
    dir=os.getcwd()
    # 要导出的 清洗后的 txt csv文件名称及路径
    self.export_txtfile_path=dir+'/data/%s_%s_new_etl.txt'%(self.client_nmbr,self.batch)
    #要导出的 清洗后的 excel 文件名称及路径
    self.export_etl_xlsx_file=dir+'/data/%s_%s_new_etl.xlsx'%(self.client_nmbr,self.batch)
    #加载 要读取 清洗的excel 文件
    self._rawdata=pd.read_excel(self.file_path,sheet_name=self.sheet_name,header=header,encoding=encoding,parse_dates=[self.date_filed],dtype={self.phone_filed:np.str})
    try:
      print("parse date column")
      #预处理 apply_time 列
      self._rawdata["apply_time"]=pd.to_datetime(self._rawdata[self.date_filed],format='%Y-%m-%d',errors='ignore')
    except:
      print("parse  date error,please check")
      #预处理  应用场景 列
    self._rawdata["sense_code"] = self.sense_code


# gid 的生成，五要素生成 md5
  def  md5_gid(self,five_features):
    md=hashlib.md5()
    txt_fix=str(five_features)
    #print(txt_fix)
    md.update(txt_fix.encode('utf-8'))
    res_md5 = md.hexdigest()
   # print(res_md5)
    return res_md5

# 将gid 追加到 最后要输出的 dataframe，被引用
  def md5_gid_df(self,df):
      #md5_filed_list=("realname","certid","mobile","card","apply_time")
      #new_df["apply_time"] = new_df["apply_time"].astype(np.str).apply(lambda x: str(x)[:10])
      #new_df = df[[id for id in md5_filed_list]]
      df["apply_time"]=df["apply_time"].astype(np.str).apply(lambda x:str(x)[:10])
      new_df=df[["realname","certid","mobile","card","apply_time"]]
      new_col=pd.DataFrame()
      new_col["one"] = new_df.apply(lambda row:str(row['realname']).replace(' ','') + str(row['certid']).replace(' ','').upper() + str(row['mobile']).replace(' ','') + str(row['card']).replace(' ','') + str(row['apply_time']).replace(' ','')[:10],axis=1)
      df['gid']=new_col["one"].apply(lambda row:self.md5_gid(str(row)))
      return df

  # 生成 最后要输出的 dataframe,
  def re_construct_df_byraw_header_loc_list(self):
    print(self._rawdata.head())
    #(0,1) (1,2) (2,3) (3,5)
    for raw_loc,raw_filed_index in enumerate(self.raw_header_loc):
      #(0,gid)(1,realname)(2,certid)(3,mobile)
         for f_index,filed_name in enumerate(self.std_filed_list):
          if f_index==raw_filed_index:
            print("*****")
            origin_col=self._rawdata.columns[raw_loc]
            print("原始列名： %s ,标准列名： %s"%(origin_col,filed_name))
            print(self._rawdata[self._rawdata.columns[raw_loc]].head(2))
            print("*****")
            self._rawdata[filed_name]=self._rawdata[self._rawdata.columns[raw_loc]]
    df=pd.DataFrame()
    new_raw_filed_list=list(self._rawdata.columns)
    for f_index, filed_name in enumerate(self.std_filed_list):
      if  filed_name in new_raw_filed_list:
        df[filed_name]=self._rawdata[filed_name]
      else :
        df[filed_name]=""
    self.md5_gid_df(df)
    return df

    # 生成 最后要输出的 dataframe 依赖 raw_header_loc_char_dict
    #{'realname':'G','certid':'I','mobile':'J','apply_time':'C' }
  def re_construct_df_by_raw_header_loc_char_dict(self):
      print(self._rawdata.head())
      # (0,1) (1,2) (2,3) (3,5)
      for raw_std_filed, raw_sheet_word in self.raw_header_loc_char_dict.items():
        # (0,gid)(1,realname)(2,certid)(3,mobile)
        for s_index, sheet_col_word in enumerate(self.sheet_char_index):
          if sheet_col_word.upper() == raw_sheet_word.upper():
            print("*****")
            origin_col = self._rawdata.columns[s_index]
            print("原始列名： %s ,标准列名： %s" % (origin_col, raw_std_filed))
            print(self._rawdata[self._rawdata.columns[s_index]].head(2))
            print("*****")
            self._rawdata[raw_std_filed] = self._rawdata[self._rawdata.columns[s_index]]
      df = pd.DataFrame()
      new_raw_filed_list = list(self._rawdata.columns)
      for f_index, filed_name in enumerate(self.std_filed_list):
        if filed_name in new_raw_filed_list:
          df[filed_name] = self._rawdata[filed_name]
        else:
          df[filed_name] = ""
      self.md5_gid_df(df)
      return df

    #读取 excel文件
  def  load_label_excel_file(self,header=0,encoding='utf-8',sep='\t'):
    raw_data=pd.read_excel(self.file_path,header=header,encoding=encoding,parse_dates=[self.date_filed],dtype={self.phone_filed:np.str})
    raw_data["sense_code"]=self.sense_code
    return raw_data

    #读取 csv 文件
  def  load_label_csv_file(self,header=0,encoding='utf-8',sep='\t'):
    raw_data=pd.read_csv(self.file_path,header=header,encoding=encoding,sep=sep,parse_dates=[self.date_filed],dtype={self.phone_filed:np.str})
    raw_data["sense_code"]=self.sense_code
    return raw_data

#对时间 列的处理
  def  etl_date(self,raw_Data,date_filed):
    try:
      nwedate=pd.to_datetime(raw_Data[date_filed],format='%Y-%m-%d',errors='ignore')
     # print(nwedate.head())
    except :
      print("error")
    return nwedate

# 生成最终的 txt 和 excel 文件  保存在磁盘
  def  save_export_files(self,df,is_export_excel=False,csv_header=False,encoding='utf-8',sep='\t',index=False):

    df.to_csv(self.export_txtfile_path,encoding=encoding,header=csv_header,sep=sep,index=index)
    print("导出最终txt  文件成功，txt路径 ：%s"%self.export_txtfile_path)
    if is_export_excel :
      df.to_excel(self.export_etl_xlsx_file,encoding=encoding,index=index)
      print("导出最终excel 文件成功，excel 路径： %s"%self.export_etl_xlsx_file)

    #上传到ftp 上
  def  ftp_to_sever(self,host,ip,tpath):
    print("error")

#执行 hive 入库操作
  def  _exec_hive(self):
    print("error")



if __name__ == '__main__':
    file_path="data/AA98p1_20180529.xlsx"
    export_txtfile_path="data/AA98p1_20180529.txt"
    export_etl_xlsx_file="data/AA98p1_201805.xlsx"
    sheetname='Sheet1'
    sense_code="线上现金分期"
    dataFiled="app_date"
    phoneFiled="phone1"
    client_nmbr="AA98"
    batch='p1'
    raw_header_loc=(5,1,2,3)
    raw_header_loc_num_list = ()
    raw_header_loc_char_dict= {'realname':'b','certid':'c','mobile':'d','card':'*','apply_time':'a','y_label':'*','apply_amount':'*','apply_period':'*','overdus_day':'*','sense_code':'*' }
    etl=ETL_Label(file_path,raw_header_loc_char_dict,sense_code,dataFiled,phoneFiled,sheetname,client_nmbr,batch)
    #fd=etl.re_construct_df()
    #news=etl.md5_gid_df(fd)
    df=etl.re_construct_df_by_raw_header_loc_char_dict()
    etl.save_export_files(df,is_export_excel=True,csv_header=True)
    #fd.to_excel(res_file,encoding='utf-8')
    print(df.head())



def  ts():
    print("last code")
    # path="data/AA95p1_20test.xlsx"
    # sheetname='mo'
    # sense_code="线下消费分期"
    # dataFiled="申请时间"
    # phoneFiled="手机号"
    # res_file_path ="data/AA95p1_20test.txt"
    # client_nmbr = "AA95"
    # batch = 'p1'
    # raw_header_loc = (1, 2,3,5)
    # path = "data/AA51p13_201805.xlsx"
    # res_file_path = "data/AA51p13_201805.txt"
    # res_file = "data/AA51p13_2018_etl.xlsx"
    # sheetname = 'Sheet1'
    # sense_code = "线上现金分期"
    # dataFiled = "DATE_DECISION"
    # phoneFiled = "解密手机号"
    # client_nmbr = "AA51"
    # batch = 'p13'
    #"realname","certid","mobile","card","apply_time","y_label"