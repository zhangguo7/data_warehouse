# -*- coding:utf-8 -*-
"""
推送附件信息
help html:https://pupapi.com/project/show/index?version_id=86&project_id=54&id=182
author:Li Zhenlin
"""
import logging,logging.config
import configparser
import json

import pandas as pd
import requests
import numpy as np
import hashlib

import time

from post2api import tools


def compile_json_data(df):
    """组合json数组

    :param df:API_1中提取的数据框
    :return:
    """
    # 列名的第一个字段是gid，不用传输
    col_name = list(df.columns)
    col_name.pop(0)
    post_vars = col_name
    sample_list = []
    for i, sample in df.iterrows():
        data_list = list()
        for var in post_vars:
            data_list.append([var, sample[var]])
        sample = dict(data_list)
        sample_list.append(sample)
    full_json = {
        'operateType': '1',
        'vos': sample_list
    }
    post_json = json.dumps(full_json)

    return post_json


def api(encryption_key, rd, json_data, url):

    headers = {
        "content-type": "application/json",
        "encryptionKey": encryption_key,
        'rd': rd
    }

    # 正式
    # url = tools.read_url(table_name)
    # print(url)
    # url = "http://sync.adas.com/admin/v1/sync/commercialzone_sample"
    # 测试
    # url = "http://192.168.89.241:8080/admin/v1/sync/commercialzone_sample"
    r = requests.post(url, data=json_data, headers=headers, timeout=40)
    code = r.json()['code']
    # print('传递状态码:%s' % code)
    info = r.text
    # print(r.text)
    return code, info


def md5(rd, pk):
    # 使用MD5函数对public key加密
    m = hashlib.md5()
    m.update((rd + pk).encode())
    return m.hexdigest()


def _read_begin_id(cfg_name):
    # 从配置文件中读取起始id
    post_cfg = configparser.ConfigParser()
    post_cfg.read('anti_attach.cfg')
    begin_id = post_cfg[cfg_name]['begin_id']
    return int(begin_id)


def post_data():
    """数传数据到  http://192.168.89.241:8080/admin/v1/sync/，主函数

    :param db_engine:app暂存表的数据引擎
    :return:
    """
    total = 5000000
    j = int(total/500)

    public_key = 'fbf2c2ec-6853-4281-8661-6c3e9017193d'
    url = tools.read_url(table_name)

    suc = 0;fail = 0;total = 0
    for n in range(j):

        begin_id = n * 500 + 1
        end_id = (n + 1) * 500
        select_data = "SELECT * FROM %s where gid BETWEEN %d and %d" \
                      % (table_name, begin_id, end_id)
        # print(select_data)
        # select_data = "SELECT * FROM %s WHERE gid between 4936676 and 4937175 ORDER BY gid" % table_name
        chunk_df = pd.read_sql(sql=select_data, con=origin_engine)

        time_vars = ['addDate', 'uploadDate', 'receiveDate', 'updateDate']
        for var in time_vars:
            chunk_df[var] = chunk_df[var].astype(str)
        # def dt2str(x):
        #     try:
        #         return str(x)
        #     except:
        #         return ''

        json_data = compile_json_data(chunk_df)
        # 最多传输三次，如果都失败，则记录下传输内容
        for k in range(3):
            rd = "".join(list(map(str, np.random.randint(0, 9, size=32))))
            encryption_key = md5(str(public_key + rd), public_key)
            res_code, info = api(encryption_key, rd, json_data, url)
            if res_code == '1':
                write_cfg(chunk_df, table_name, 'gid')
                suc += len(chunk_df)
                logging.info('succeed obs. %d '%suc)
                break
            if k == 2:
                with open('err_%s.txt' % table_name, mode='a') as file:
                    min_gid = min(chunk_df['gid'])
                    max_gid = max(chunk_df['gid'])
                    # file.write(info + '\r\n' + str(min_gid) + ' to ' + str(max_gid) + '传输错误' + '\r\n')
                    file.write('%d,%d\n'%(min_gid,max_gid))
                    fail+=len(chunk_df)
                    logging.warning('failure obs. %d, from %d to %d,res_code:%s,info:%s' % (fail,min_gid,max_gid,res_code,info))

                with open('%s_json.txt' % table_name, mode='a', encoding='utf8') as file:
                    file.write(json_data)
        total+=len(chunk_df)
        logging.info('round %d,total = %d,secceed = %d,failure = %d'%(n,total,suc,fail))

def write_cfg(load_df, sec_name, based_id='id'):
    """
    将配置文件相关数据回写
    :param load_df: 写入数据库的data
    :param sec_name: 配置文件section的名称
    :param based_id: begin_id的依据字段，默认为id
    :return:
    """
    id_cfg = configparser.ConfigParser()
    id_cfg.read('anti_attach.cfg')
    max_id = max(load_df[based_id])
    post_len = len(load_df)
    total_len = int(id_cfg[sec_name]['total_length'])
    total_len += post_len
    id_cfg.set(sec_name, "begin_id", str(max_id))
    id_cfg.set(sec_name, "post_length", str(post_len))
    id_cfg.set(sec_name, "total_length", str(total_len))
    id_cfg.write(open("post_attach.cfg", "w"))
    print('本次成功%d条，合计成功%d条' % (post_len, total_len))

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('db.cfg')
    logging.config.fileConfig('log.cfg')

    # print(db_cfg.sections())
    origin_engine = tools.mysql_engine(**db_cfg['dw_anti_fraud'])
    table_name = 'anti_attach'

    t1 = time.time()
    try:
        post_data()
    except:
        pass
    t2 = time.time()
    print('%s传输完毕，用时%.2f' % (table_name, ((t2-t1)/60)))

