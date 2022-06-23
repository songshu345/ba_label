# coding = utf-8
import csv
import json
import logging
import pandas as pd
from loguru import logger
from conf.file_conf import data_path, root_path  # 路径查找

# 文件
BA_label_file = root_path + 'label_report/标签报告_for_bac/BA_label_yx_2021.txt'
betweenness_file = root_path + 'yx_betweenness/yx_q4_betweenness.txt'


# 标签进一步清理之后，再进行直接的文件导入

def label_betweenness():
    BA_label_dd_q = pd.read_csv(BA_label_file, encoding='utf-8',
                                dtype={'emp_code': str},
                                sep=',')
    BA_label_dd_q = BA_label_dd_q.query('label == "Q4"')
    BA_betweenness = pd.read_csv(betweenness_file, encoding='utf-8', sep=',', dtype={'emp_code': str})
    # BA_label_dd_q = BA_label_dd_q[
    #     ['emp_code', 'to_emp_code', 'subdomain_name', 'department_code', 'parent_realm_name', 'parent_department_code','dept_level']]
    # 关联BA的中介中心度
    BA_label_betweenness = pd.merge(
        BA_label_dd_q,
        BA_betweenness,
        left_on='emp_code',  # 修改
        right_on='emp_code',
        how='left')
    # print(BA_label_betweenness.columns)
    # BA_label_betweenness = BA_label_betweenness.rename(columns={'emp_code_x': 'emp_code'})
    # BA_label_betweenness.drop(BA_label_betweenness[['to_emp_code', 'emp_code_y']], axis=1,inplace=True)
    print(BA_label_betweenness)

    # BA_label_betweenness['rank_c'] = BA_label_betweenness.groupby('subdomain_name')['centrality'].rank(
    #     ascending=False)
    BA_label_betweenness.sort_values(['label', 'subdomain_name', 'parent_realm_name', 'emp_code', 'centrality'],
                                     ascending=[1, 1, 1, 1, 0], inplace=True)
    BA_label_betweenness['rank_c'] = BA_label_betweenness.groupby(['label', 'subdomain_name'])['centrality'].rank(ascending=False)
    BA_label_betweenness = BA_label_betweenness[
        ['label', 'subdomain_name', 'emp_code', 'centrality','rank_c']]
    BA_label_betweenness.drop_duplicates(inplace=True)  # 去重
    # BA_label_betweenness['rank_c'] = BA_label_betweenness.groupby('emp_code', axis=0)['centrality'].rank(ascending=False)
    return BA_label_betweenness


BA_label_betweenness = label_betweenness()
BA_label_betweenness.to_excel(r'D:\treasture\label_BA\label_report\标签报告_for_bac\5BG分季度标签\yx_label\yx_label_4.xlsx',
                              encoding='utf-8', index=False)
print(BA_label_betweenness)
