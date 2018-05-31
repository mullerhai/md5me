import pandas  as pd
import  numpy  as  np
from ftps_client import Ftps_client
import  hashlib
import os
from  configparser import  ConfigParser
import  ast
import traceback
import logging
import paramiko
import sys
import  time
import  threading
logger = logging.getLogger(
    name=__name__,
)
class ETL_Label:
  logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  logger = logging.getLogger(__name__)
  #初始化 会读取文件 ，并对文件做最初步的清洗
  def __init__(self,conf_sec='section_etl_excel_label',config_file='etl.conf',header=0,is_csv=False,csv_sep='\t',raw_header_loc=()):
    #raw_header_loc_char_dict,sense_code,date_filed,phone_filed,sheet_name='Sheet1',client_nmbr="AA00",batch="p0" header=0,encoding='gbk',
    #标准的 入库 列名 顺序  section_etl_excel_label_AA100_p1
    self.std_filed_list=("gid","realname","certid","mobile","card","apply_time","y_label","apply_amount","apply_period","overdus_day","sense_code")
    col_index=(f  for f in range(0,11))
    #标准的 入库 列名加索引字典
    self.std_filed_dict=dict(zip(col_index,self.std_filed_list))
    #excel sheet表格 列单元 索引顺序对应的默认字母
    self.sheet_char_index=('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T')
   # 要读取的配置文件
    self.config_file=config_file
    #配置文件解析器
    self.config_parser=ConfigParser()
    self.config_parser.read(self.config_file,encoding='utf-8-sig')
    #配置文件要读取 的 配置 片段
    self.config_etl_label_section=conf_sec
    # 读取 要处理的文件路径 相对路径
    self.file_path =self.config_parser.get(conf_sec,'file_path')
    # excel 表格 的样本所在的 sheet 名称
    self.sheet_name = self.config_parser.get(conf_sec,'sheet_name')
    # client_nmbr
    self.client_nmbr=self.config_parser.get(conf_sec,'client_nmbr')
    self.batch=self.config_parser.get(conf_sec,'batch')
    #self.hive_host =self.config_parser.get(conf_sec, 'hive_host')
    #场景
    self.sense_code=self.config_parser.get(conf_sec,'sense_code')
    #标准列名 apply_time对应的原始 excel 表格 业务列名
    self.date_filed=self.config_parser.get(conf_sec,'date_filed')
    #标准列名 mobile  对应的原始 excel 表格 业务列名
    self.phone_filed=self.config_parser.get(conf_sec,'phone_filed')
    self.csv_sep=csv_sep
    l_c_dict_str=self.config_parser.get(conf_sec, 'raw_header_loc_char_dict')
    # 标准的 入库 列名 与 excel sheet表格 列单元 索引顺序对应的默认字母 的对应字典，不存在用 * 星号 表示
    self.raw_header_loc_char_dict = ast.literal_eval(l_c_dict_str)
    #读取的 excel  header
    self.header=header
    #读取的文件  编码
    self.encoding = self.config_parser.get(conf_sec,'encoding')
    # 实际 清洗excel 文件 原始列名 与标准的 入库 列名 索引的对应字典
    self.raw_header_loc = raw_header_loc
    self.Ftps=None
    #获取当前执行路径
    dir=os.getcwd()
    # 要导出的 清洗后的 txt csv文件名称及路径
    self.export_txtfile_path=dir+'/data/%s_%s_new_etl.txt'%(self.client_nmbr,self.batch)
    #要导出的 清洗后的 excel 文件名称及路径
    self.export_etl_xlsx_file=dir+'/data/%s_%s_new_etl.xlsx'%(self.client_nmbr,self.batch)
    #加载 要读取 清洗的excel 文件,默认读取 excel
    if is_csv ==False:
      self._rawdata=pd.read_excel(self.file_path,sheet_name=self.sheet_name,header=self.header,encoding=self.encoding,parse_dates=[self.date_filed],dtype={self.phone_filed:np.str})
    else:
      self._rawdata = pd.read_csv(self.file_path, sep=self.csv_sep, header=self.header, encoding=self.encoding,parse_dates=[self.date_filed], dtype={self.phone_filed: np.str})

    try:
      logger.info(msg="parse date column")
      #预处理 apply_time 列
      self._rawdata["apply_time"]=pd.to_datetime(self._rawdata[self.date_filed],format='%Y-%m-%d',errors='ignore')
    except:
      logger.info(msg="parse  date error,please check")
      #预处理  应用场景 列
    self._rawdata["sense_code"] = self.sense_code


# gid 的生成，五要素生成 md5
  def  md5_gid(self,five_features,encoding='utf-8'):
    md=hashlib.md5()
    txt_fix=str(five_features)
    #print(txt_fix)
    md.update(txt_fix.encode(encoding))
    res_md5 = md.hexdigest()
   # print(res_md5)
    return res_md5

# 将gid 追加到 最后要输出的 dataframe，被引用
  def md5_gid_df(self,df):
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
            logger.info(msg="*****")
            origin_col=self._rawdata.columns[raw_loc]
            logger.info(msg="原始列名： %s ,标准列名： %s"%(origin_col,filed_name))
            print(self._rawdata[self._rawdata.columns[raw_loc]].head(2))
            logger.info(msg="*****")
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
            logger.info(msg="*****")
            origin_col = self._rawdata.columns[s_index]
            logger.info(msg="原始列名： %s ,标准列名： %s" % (origin_col, raw_std_filed))
            print(self._rawdata[self._rawdata.columns[s_index]].head(2))
            logger.info(msg="*****")
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
      logger.info(msg="error")
    return nwedate

# 生成最终的 txt 和 excel 文件  保存在磁盘
  def  save_export_files(self,df,is_export_excel=False,csv_header=False,encoding='utf-8',sep='\t',index=False):

    df.to_csv(self.export_txtfile_path,encoding=encoding,header=csv_header,sep=sep,index=index)
    logger.info(msg="导出最终txt  文件成功，txt路径 ：%s"%self.export_txtfile_path)
    if is_export_excel :
      df.to_excel(self.export_etl_xlsx_file,encoding=encoding,index=index)
      logger.info(msg="导出最终excel 文件成功，excel 路径： %s"%self.export_etl_xlsx_file)

    #上传到ftp 上 mac  unix dir_sep is /, mayby windows system is  \
  def  put_etlfile_ftp(self,config_ftp_sec='sec_ftps_login',upload_etl_file='',dir_sep='/'):
    host=self.config_parser.get(config_ftp_sec,'ftp_host')
    user=self.config_parser.get(config_ftp_sec,'ftp_user')
    pwd=self.config_parser.get(config_ftp_sec,'ftp_pwd')
    port=self.config_parser.get(config_ftp_sec,'ftp_port')
    server_path=self.config_parser.get(config_ftp_sec,'ftp_server_path')
    if upload_etl_file=='':
      upload_etl_file=self.export_txtfile_path
    server_file_name=str(upload_etl_file).split(dir_sep)[-1]
    logger.info(msg="server_file_name : %s host: %s , user : %s ,pwd: %s ,port : %s ,server_path : %s "%(server_file_name,host,user,pwd,port,server_path))
    try:
      logger.info(msg="开始准备上传 %s 到 ftp 服务器" %upload_etl_file)
      cli = Ftps_client(host, user, pwd, port)
      cli.login(2, True)

      cli.ftpUploadLocalFile(upload_etl_file,server_path,server_file_name)
      logger.info(msg="文件%s 上传成功"%upload_etl_file)
      self.Ftps=cli
    except Exception as ex :
      logger.info(msg="上传失败请检查 ,error %s"%ex)
      traceback.print_exc()

#获取 跳板机 ssh server
  def  get_jumper_ssh_session(self,sec_remote_host = 'sec_remote_host'):
    self.jumper_ssh_session = paramiko.SSHClient()
    self.jumper_ssh_session.set_missing_host_key_policy(
      paramiko.AutoAddPolicy(),
    )
    try:
      self.jumper_ssh_session.connect(
        hostname=self.config_parser.get(sec_remote_host, 'jump_host'),
        port=self.config_parser.get(sec_remote_host, 'jump_port'),
        username=self.config_parser.get(sec_remote_host, 'jump_user'),
        password=self.config_parser.get(sec_remote_host, 'jump_pwd')
      )
      if self.jumper_ssh_session != None:
        logger.info(msg="jumper_ssh_session get successfully")
      else:
        logger.error(msg="failed to get jumper_ssh_session ")
    except Exception as ex:
      logger.error(msg="failed to get jumper_ssh_session by error %s "%ex)
      raise

# 在跳板机执行命令
  def  exec_jump_hive_command(self,hive_host,cmd_str):
    if self.jumper_ssh_session != None:
        logging.info(msg="jump_hive host %s will exec__command  %s"%(hive_host,cmd_str))
        final_cmd_str='ssh  %s -t %s'%(hive_host,cmd_str)
        stdin, stdout, stderr = self.jumper_ssh_session.exec_command(final_cmd_str)
        channel = stdout.channel
        status = channel.recv_exit_status()
        if status ==0:
          logging.info(msg="exec_jump_hive_command %s successfully"%cmd_str)
          return status
        else:
          logging.error(msg="failed to  exec_jump_hive_command %s "%cmd_str)
          return 1

    # 执行 hive 入库操作
  def _exec_data_insert_hive(self, hive_host, exec_hive_cmd):
      logger.info(msg=exec_hive_cmd)
      logger.info(msg="will insert data please  waiting for  some minutes !!! util to console feed up ")
      try:
        exe_status = self.exec_jump_hive_command(hive_host, exec_hive_cmd)
        if exe_status == int(0):
          logger.info(msg="exec third command insert  data to hive database  successfully" )
        else:
          logger.error(msg="failed insert data to hive database ,please check !!!" )
      except Exception as ex:
        logger.error(msg="failed insert  data to hive database ,please check the error %s !!!" %ex)
        raise

        #在跳板执行 从ftp服务器拉取 文件 到 转移文件路径 到执行脚本入库
  def exec_ftp_get_mv_insert_command(self,sec_remote_host='sec_remote_host',dir_sep='/'):
    self.get_jumper_ssh_session()
    hivehost =self.config_parser.get(sec_remote_host,'hive_host' )
    hive_server_dir_path =self.config_parser.get(sec_remote_host,'hive_server_dir_path')
    ftpget_login =self.config_parser.get(sec_remote_host, 'ftpget_login')
    ftp_file_dir=self.config_parser.get(sec_remote_host, 'ftp_file_dir')
    ftp_etl_file=str(self.export_txtfile_path).split(dir_sep)[-1]
    try:
      ftp_get_cmd='%s/%s/%s'%(ftpget_login,ftp_file_dir,ftp_etl_file)
      mv_file_cmd = 'mv  %s  %s ' % (ftp_etl_file, hive_server_dir_path)
      exec_hive_cmd='sh   %s/load_label.sh  %s  %s  %s' % (hive_server_dir_path,ftp_etl_file,self.client_nmbr,self.batch)
      logger.info(msg=ftp_get_cmd)
      #ftp_get_cmd ='ssh 172.16.16.31
      status=self.exec_jump_hive_command(hivehost,ftp_get_cmd)
      if  status==int(0):
        logger.info(msg=mv_file_cmd)
        mv_status=self.exec_jump_hive_command(hivehost,mv_file_cmd)
        logger.info(msg="exec second command  mv file command:  success " )
        if  mv_status==int(0):
          self._exec_data_insert_hive(hivehost,exec_hive_cmd)
      else :
        logging.warning(msg="first command is failed")
    except Exception as ex:
      logging.error(msg=ex)




def etl_sample_for_AA99_p1_():
  conf_sec = 'section_etl_excel_label_AA100_p1'
  etl = ETL_Label(conf_sec=conf_sec)
  dic = etl.config_parser.get(conf_sec, 'raw_header_loc_char_dict')
  print(dic)
  dics = ast.literal_eval(dic)
  for k, v in dics.items():
    print('k: %s, V: %s' % (k, v))
  df = etl.re_construct_df_by_raw_header_loc_char_dict()
  etl.save_export_files(df, is_export_excel=True, csv_header=False)
  print(df.head())
  etl.put_etlfile_ftp()
  etl.exec_ftp_get_mv_insert_command()

if __name__ == '__main__':
  etl_sample_for_AA99_p1_()
