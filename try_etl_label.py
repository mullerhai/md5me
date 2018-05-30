from etl_label import  ETL_Label
import  pandas as  pd
import  numpy as np

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
    etl=ETL_Label(file_path,sheetname,sense_code,dataFiled,phoneFiled,raw_header_loc_char_dict,raw_header_loc_num_list,client_nmbr,batch,export_etl_xlsx_file=export_etl_xlsx_file)
    #fd=etl.re_construct_df()
    #news=etl.md5_gid_df(fd)
    df=etl.re_construct_df_by_raw_header_loc_char_dict()
    etl.save_export_files(df,export_txtfile_path,is_export_excel=True,header=True)
    #fd.to_excel(res_file,encoding='utf-8')
    print(df.head())

