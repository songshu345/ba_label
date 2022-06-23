# coding = utf-8
import csv
import json
import logging
import pandas as pd
from loguru import logger
from conf.file_conf import data_path, root_path  # 路径查找

# root_path = 'D:/treasture/label_BA/'
# data_path = root_path + 'data/'

# 路径
org_file_name = data_path + '/yx_org_2021.txt'  # 组织架构文件
dx_f1 = '/2021Q1_yx.txt'
dx_f2 = '/2021Q2_yx.txt'
dx_f3 = '/2021Q3_yx.txt'
dx_f4 = '/2021Q4_yx.txt'
dx_file_name = root_path + 'dx_yx_data/'  # 路径
jf_file_name = root_path + 'dx_yx_data/' + 'yx_2021_job_family.txt'
ona_label = root_path + 'dx_yx_data/' + 'yx_ona3.txt'


# drop函数默认删除行，列需要加axis=1
def convert_org_path_to_line(org_file_name, dept_code_path='dept_code_path'):
    df_org = pd.read_csv(org_file_name, encoding='utf-8', dtype={'dept_code': str, 'dept_code_path': str}, sep='\t')

    df_org_all = df_org.drop(dept_code_path, axis=1).join(
        df_org[dept_code_path].str.split('/', expand=True).stack().reset_index(level=1, drop=True).rename('pt_code')
    ).query("pt_code != ''")

    return df_org_all


# 组织名称拆分
def convert_org_name_to_line(org_file_name, dept_name_path='dept_name_path'):
    df_org = pd.read_csv(org_file_name, encoding='utf-8', dtype={'dept_code': str, 'dept_code_path': str}, sep='\t')

    df_org_name = df_org.drop(dept_name_path, axis=1).join(
        df_org[dept_name_path].str.split('_', expand=True).stack().reset_index(level=1, drop=True).rename('pt_name'))
    return df_org_name


def org_name_dept(org_file_name):
    df_org = pd.read_csv(org_file_name, encoding='utf-8',
                         dtype={'dept_code': str, 'mapping_top_04_dept_code': str,
                                'mapping_top_03_dept_code': str,
                                'mapping_top_05_dept_code': str,
                                'mapping_top_06_dept_code': str,
                                'mapping_top_07_dept_code': str,
                                'dept_code_path': str}, sep='\t')
    # df_org = df_org[df_org["dept_code_path"].str.contains("104493")]  # 到店事业群
    return df_org


df_org = org_name_dept(org_file_name)


# 增加季度标签
def label_dx():
    DX_Q1_dj = pd.read_csv(dx_file_name + dx_f1, sep='\t',
                           dtype={'emp_code': str, 'to_emp_code': str, 'dept_code': str,
                                  'to_dept_code': str})  # csv文件的分句结果好于txt
    DX_Q1_dj['label'] = 'Q1'
    DX_Q2_dj = pd.read_csv(dx_file_name + dx_f2, sep='\t',
                           dtype={'emp_code': str, 'to_emp_code': str, 'dept_code': str,
                                  'to_dept_code': str})  # csv文件的分句结果好于txt
    DX_Q2_dj['label'] = 'Q2'
    DX_Q3_dj = pd.read_csv(dx_file_name + dx_f3, sep='\t',
                           dtype={'emp_code': str, 'to_emp_code': str, 'dept_code': str,
                                  'to_dept_code': str})  # csv文件的分句结果好于txt
    DX_Q3_dj['label'] = 'Q3'
    DX_Q4_dj = pd.read_csv(dx_file_name + dx_f4, sep='\t',
                           dtype={'emp_code': str, 'to_emp_code': str, 'dept_code': str,
                                  'to_dept_code': str})  # csv文件的分句结果好于txt
    DX_Q4_dj['label'] = 'Q4'
    return DX_Q1_dj, DX_Q2_dj, DX_Q3_dj, DX_Q4_dj


DX_Q1_dj, DX_Q2_dj, DX_Q3_dj, DX_Q4_dj = label_dx()


# 数据量较大，用于合并数据
def dx_messge_merge(DX_Q1_dj, DX_Q2_dj, DX_Q3_dj, DX_Q4_dj):
    DX_Q1234_dj = pd.concat([DX_Q1_dj, DX_Q2_dj, DX_Q3_dj, DX_Q4_dj])  # 全年四个季度数据合并

    return DX_Q1234_dj


# 是否为结点负责人（向上汇总）
# 只要在子领域出现了，就向上一层查找：在子领域标签中再次查找该级部门，直到查找不到

DX_Q1234_dj = dx_messge_merge(DX_Q1_dj, DX_Q2_dj, DX_Q3_dj, DX_Q4_dj)


# print(DX_Q1234_dj)


#
# # DX_Q1234_dd.to_excel('DX_Q1234_dd_0417.xlsx', encoding='utf-8', index=False)

# 分ba和ab进行识别，防止去掉结点
def DX_BA_sr(DX_Q1234_dj):
    DX_1234_new = []
    # 去除无效数据
    # 删除符合条件的指定行，并替换原始df
    DX_Q1234_dj.drop(DX_Q1234_dj[(DX_Q1234_dj.qua_r_num == 0)].index, inplace=True)
    DX_Q1234_dj = DX_Q1234_dj.query('r_day_cnt != 0')  # 筛选r
    DX_Q1234_dj['key_value'] = (DX_Q1234_dj['s_day_cnt'] + DX_Q1234_dj['r_day_cnt']) / 2  # 新增Key列
    DX_Q1234_dj['A-to-B'] = DX_Q1234_dj['emp_code'] + '' + DX_Q1234_dj['to_emp_code']  # A to B
    DX_Q1234_dj['B-to-A'] = DX_Q1234_dj['to_emp_code'] + '' + DX_Q1234_dj['emp_code']  # B to A
    DX_Q1234_dj['key_value'] = DX_Q1234_dj['key_value'].map(lambda x: str(x))
    DX_Q1234_dj['A-to-B_key'] = DX_Q1234_dj["A-to-B"].str.cat(DX_Q1234_dj["key_value"], sep="")
    DX_Q1234_dj['B-to-A_key'] = DX_Q1234_dj["B-to-A"].str.cat(DX_Q1234_dj["key_value"], sep="")
    # DX_1234_new = DX_Q1234_dd.query('A-to-B != "0"')
    # A_B_index_label = pd.DataFrame(DX_Q1234_dd,columns=['A-to-B','key_value'])  # 创建新的数据框
    # cha
    DX_1234_1 = DX_Q1234_dj[DX_Q1234_dj['A-to-B_key'].isin(DX_Q1234_dj['B-to-A_key'])]  # AB、BA
    DX_1234_2 = DX_Q1234_dj[~DX_Q1234_dj['A-to-B_key'].isin(DX_Q1234_dj['B-to-A_key'])]  # 非AB、BA
    # 列互换
    DX_1234_2[['emp_code', 'to_emp_code']] = DX_1234_2[['to_emp_code', 'emp_code']]
    # DX_1234_2[['emp_mis_name', 'to_emp_mis_name']] = DX_1234_2[['to_emp_mis_name', 'emp_mis_name']]
    DX_1234_2[['dept_code', 'to_dept_code']] = DX_1234_2[['to_dept_code', 'dept_code']]
    DX_1234_2[['s_day_cnt', 'r_day_cnt']] = DX_1234_2[['r_day_cnt', 's_day_cnt']]
    # DX_1234_2[['s_week_cnt', 'r_week_cnt']] = DX_1234_2[['r_week_cnt', 's_week_cnt']]
    # DX_1234_2[['s_month_cnt', 'r_month_cnt']] = DX_1234_2[['r_month_cnt', 's_month_cnt']]
    # DX_1234_2[['qua_s_num', 'qua_r_num']] = DX_1234_2[['qua_r_num', 'qua_s_num']]
    DX_1234_2 = DX_1234_2[
        ['emp_code', 'to_emp_code', 'dept_code', 'to_dept_code', 'label', 's_day_cnt', 'r_day_cnt']]
    DX_1234_new = pd.concat([DX_Q1234_dj, DX_1234_2])  # 上述两种情况合并
    #
    return DX_1234_new


DX_1234_new = DX_BA_sr(DX_Q1234_dj)


# 关联序列(时间Label)
def jf(jf_file_name, DX_1234_new):
    jf = pd.read_csv(jf_file_name, sep='\t', dtype={'emp_code': str})
    DX_jf = pd.merge(
        DX_1234_new,
        jf,
        left_on=['emp_code', 'label'],
        right_on=['emp_code', 'time_label'],
        how='left'
    )
    DX_jf = DX_jf.rename(columns={'emp_code_x': 'emp_code'})  # 修改列名
    DX_jf = DX_jf[
        ['emp_code', 'to_emp_code', 'emp_mis_name', 'to_emp_mis_name', 'dept_code', 'to_dept_code', 's_day_cnt',
         'r_day_cnt', 'label', 'emp_job_family_desc']]
    # 's_week_cnt', 'r_week_cnt','s_month_cnt', 'r_month_cnt', 'qua_s_num', 'qua_r_num',

    DX_jf = DX_jf.query('emp_job_family_desc == "BA"')
    DX_jf.drop_duplicates(subset=None, keep='first', inplace=True)
    return DX_jf


DX_jf = jf(jf_file_name, DX_1234_new)


# print(df_org.columns)


# print(DX_jf)
# DX_jf.to_excel('D:/treasture/label_BA/label_report/标签报告_for_bac/dj_BA_DX_jf_2021.xlsx', encoding='utf-8', index=False)
# 关联各级部门
def BA_dept(DX_jf, df_org):
    # 筛选出发出者为“到家事业群”
    BA_dept1 = pd.merge(
        DX_jf,
        df_org,
        left_on='dept_code',
        right_on='dept_code',
        how='left'
    )
    BA_dept1 = BA_dept1.rename(columns={'dept_code_x': 'dept_code'})
    BA_dept1 = BA_dept1.dropna(subset=["dept_code_path"])  # 删除为NAN的行
    BA_dept1 = BA_dept1[BA_dept1["dept_code_path"].str.contains("155319")]  # 买菜事业群
    BA_dept1 = BA_dept1[
        ['emp_code', 'to_emp_code', 'dept_code', 'to_dept_code', 'label', 's_day_cnt', 'r_day_cnt',
         'dept_code_path', 'dept_name_path'
         # 'mapping_top_04_dept_name', 'mapping_top_05_dept_name',
         # 'mapping_top_06_dept_name',
         # 'mapping_top_07_dept_name',
         # 'mapping_top_04_dept_code', 'mapping_top_05_dept_code', 'mapping_top_06_dept_code',
         # 'mapping_top_07_dept_code'
         ]]

    # 筛选出接收者为“到家事业群”
    BA_dept2 = pd.merge(
        BA_dept1,
        df_org,
        left_on='to_dept_code',
        right_on='dept_code',
        how='left'
    )
    # BA_dept2 = BA_dept2.rename(columns={'dept_code_x': 'dept_code', 'dept_code_path_x': 'dept_code_path_s',
    #                                     'dept_name_path_x': 'dept_name_path_s', 'dept_code_path_y': 'dept_code_path_r',
    #                                     'dept_name_path_y': 'dept_name_path_r'})
    print(BA_dept2.columns)
    BA_dept2 = BA_dept2.rename(
        columns={'dept_code_path_x': 'dept_code_path', 'dept_code_path_y': 'dept_code_path_r',
                 'dept_code_name_x': 'dept_code_name', 'dept_code_name_y': 'dept_code_name_r',
                 'dept_code_x': 'dept_code', 'dept_code_y': 'dept_code_r'})  # 修改列名
    BA_dept2 = BA_dept2.dropna(subset=["dept_code_path_r"])  # 删除为NAN的行
    BA_dept2 = BA_dept2[BA_dept2["dept_code_path_r"].str.contains("155319")]  # 到店事业群
    BA_dept2 = BA_dept2[
        ['emp_code', 'to_emp_code', 'dept_code_path_r',
         # 'to_dept_code',
         'label',  # 's_day_cnt', 'r_day_cnt',
         'mapping_top_03_dept_code', 'mapping_top_03_dept_name', 'index']]

    BA_dept2.dropna(subset=['mapping_top_03_dept_code'], inplace=True)
    # # 未按部门层级排序前输出
    # # BA_dept2.to_excel('D:/treasture/label_BA/label_report/标签报告_for_bac/dj_BA_dept_wpx12.xlsx', encoding='utf-8', index=False)
    # # BA_dept.dropna  # 去空值(用法错误需改正)
    # BA_dept2 = BA_dept2.dropna(axis=0, how='any')
    # # BA_dept['index'] = pd.to_numeric(BA_dept['index'])  # 字符串转为数值型进行排序
    BA_dept2.drop_duplicates(inplace=True)  # 去重
    # BA_dept2 = BA_dept2.sort_values('index', ascending=False).groupby(['to_emp_code', 'label']).first()
    # print(BA_dept2)
    # BA_dept2.to_excel('D:/treasture/label_BA/label_report/标签报告_for_bac/dj_BA_dept_x12.xlsx', encoding='utf-8', index=False)
    return BA_dept2


BA_dept2 = BA_dept(DX_jf, df_org)


# BA_dept2 = BA_dept2.query('label == "Q1"')
# BA_dept2.to_csv('D:/treasture/label_BA/label_report/标签报告_for_bac/dj_r_dept.txt', encoding='utf-8', index=False)


# 各级标签
def BA_domain_label(BA_dept2, ona_label):
    ona_label = pd.read_csv(ona_label, sep='\t', dtype={'department_code': str, 'parent_department_code': str,
                                                        'bg_code': str})
    # 父一级标签
    BA_label = pd.merge(
        BA_dept2,
        ona_label,
        left_on='mapping_top_03_dept_code',  # 修改
        right_on='department_code',
        how='left'
    )
    BA_label = BA_label[['emp_code', 'label', 'dept_code_path_r',
                         # 'mapping_top_03_dept_name',
                         'index', 'subdomain_name', 'parent_realm_name']]
    BA_label.sort_values(['emp_code', 'label', 'dept_code_path_r', 'index'], ascending=[1, 1, 1, 0], inplace=True)
    BA_label.dropna(subset=['subdomain_name'], inplace=True)
    BA_label_1 = BA_label.groupby(['emp_code', 'label', 'dept_code_path_r']).head(1)
    BA_label_1.drop_duplicates(inplace=True)  # 去重
    # BA_label = BA_label.sort_values('index', ascending=False).groupby(['emp_code', 'label', 'subdomain_name']).first()
    return BA_label_1


BA_label_1 = BA_domain_label(BA_dept2, ona_label)
BA_label_1.to_csv('D:/treasture/label_BA/label_report/标签报告_for_bac/BA_label_yx_2021.txt', encoding='utf-8', index=False)

