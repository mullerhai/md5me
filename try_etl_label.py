from etl_label import  ETL_Label
import  pandas as  pd
import  numpy as np

if __name__ == '__main__':
    # file_path="data/AA80p2_20180530.xlsx"
    # export_txtfile_path="data/AA80p2_20180530.txt"
    # export_etl_xlsx_file="data/AA80p2_2018053.xlsx"
    # sheet_name='Sheet1'
    # sense_code="线上现金分期"
    # data_Filed="loan_data"
    # phone_Filed="mobile"
    # client_nmbr="AA80"
    # batch='p2'
    # #raw_header_loc=(5,1,2,3)
    # raw_header_loc_num_list = ()
    # raw_header_loc_char_dict= {'realname':'b','certid':'d','mobile':'c','card':'*','apply_time':'e','y_label':'*','apply_amount':'*','apply_period':'*','overdus_day':'*','sense_code':'*' }

    file_path = "data/AA99p1_20180530.xlsx"
    export_txtfile_path = "data/AA99p1_201805.txt"
    export_etl_xlsx_file = "data/AA99p1_201805.xlsx"
    sheet_name = '银联数据测试样本'
    sense_code = "线下消费分期"
    data_Filed = "账户开设时间"
    phone_Filed = "解密手机号"
    client_nmbr = "AA99"
    batch = 'p1'
    # raw_header_loc=(5,1,2,3)
    raw_header_loc_num_list = ()
    raw_header_loc_char_dict = {'realname': '*', 'certid': 'h', 'mobile': 'g', 'card': 'e', 'apply_time': 'f',
                                'y_label': '*', 'apply_amount': '*', 'apply_period': '*', 'overdus_day': '*',
                                'sense_code': '*'}

    etl=ETL_Label(file_path,raw_header_loc_char_dict,sense_code,data_Filed,phone_Filed,sheet_name,client_nmbr,batch)
    #fd=etl.re_construct_df()
    #news=etl.md5_gid_df(fd)
    df=etl.re_construct_df_by_raw_header_loc_char_dict()
    etl.save_export_files(df,is_export_excel=True,csv_header=False)
    #fd.to_excel(res_file,encoding='utf-8')
    print(df.head())

def  last_exec():
    print("last_exec")
    # file_path = "data/AA99p1_20180530.xlsx"
    # export_txtfile_path = "data/AA99p1_201805.txt"
    # export_etl_xlsx_file = "data/AA99p1_201805.xlsx"
    # sheetname = '银联数据测试样本'
    # sense_code = "线下消费分期"
    # dataFiled = "账户开设时间"
    # phoneFiled = "解密手机号"
    # client_nmbr = "AA99"
    # batch = 'p1'
    # # raw_header_loc=(5,1,2,3)
    # raw_header_loc_num_list = ()
    # raw_header_loc_char_dict = {'realname': '*', 'certid': 'h', 'mobile': 'g', 'card': 'e', 'apply_time': 'f',
    #                             'y_label': '*', 'apply_amount': '*', 'apply_period': '*', 'overdus_day': '*',
    #                             'sense_code': '*'}
