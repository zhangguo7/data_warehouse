import configparser


def mysql_engine(**kwargs):
    from sqlalchemy import create_engine
    mysql_engine = create_engine("mysql+pymysql://%s:%s@%s/%s?charset=%s"
        % (kwargs['user'],kwargs['password'],kwargs['host'],kwargs['database'],kwargs['charset']))
    return mysql_engine

def mysql_engine_without_charset(**kwargs):
    from sqlalchemy import create_engine
    mysql_engine = create_engine("mysql+pymysql://%s:%s@%s/%s"
        % (kwargs['user'],kwargs['password'], kwargs['host'],kwargs['database']))
    return mysql_engine


def mssql_engine(**kwargs):
    from sqlalchemy import create_engine
    mssql_engine = create_engine("mssql+pymssql://%s:%s@%s/%s?charset=%s"
        % (kwargs['user'],kwargs['password'],kwargs['server'],kwargs['database'],kwargs['charset']))
    return mssql_engine


def read_cfg(sec_name):
    # 读取数据库上次读写的最大id
    id_cfg = configparser.ConfigParser()
    id_cfg.read('begin_id.cfg')
    begin_id = id_cfg[sec_name]['begin_id']
    return begin_id


def write_cfg(load_df, sec_name, based_id='id'):
    """
    将配置文件相关数据回写
    :param load_df: 写入数据库的data
    :param sec_name: 配置文件section的名称
    :param based_id: begin_id的依据字段，默认为id
    :return:
    """
    id_cfg = configparser.ConfigParser()
    id_cfg.read('begin_id.cfg')
    max_id = max(load_df[based_id])
    post_len = len(load_df)
    total_len = int(id_cfg[sec_name]['total_length'])
    total_len += post_len
    id_cfg.set(sec_name, "begin_id", str(max_id))
    id_cfg.set(sec_name, "post_length", str(post_len))
    id_cfg.set(sec_name, "total_length", str(total_len))
    id_cfg.write(open("begin_id.cfg", "w"))
    print('本次成功%d条，合计成功%d条' % (post_len, total_len))


def write_cfg_with_id(load_df, max_id, sec_name):
    """
    直接给出最大id
    :param load_df: 装入数据库的df
    :param max_id: df中最大的id
    :param sec_name: 配置文件中section的名称
    :return:
    """
    id_cfg = configparser.ConfigParser()
    id_cfg.read('begin_id.cfg')
    post_len = len(load_df)
    total_len = int(id_cfg[sec_name]['total_length'])
    total_len += post_len
    id_cfg.set(sec_name, "begin_id", str(max_id))
    id_cfg.set(sec_name, "post_length", str(post_len))
    id_cfg.set(sec_name, "total_length", str(total_len))
    id_cfg.write(open("begin_id.cfg", "w"))
    print('本次装载%s条，合计载入%s条' % (post_len,total_len))


def write_cfg_with_length(sec_name, max_id, load_length):
    """
    回写配置文件，把最大id和装入长度直接传进来
    :param sec_name: 配置文件中块名称
    :param max_id: 此次读取的最大id
    :param load_length: 装入到数据中的长度
    :return:
    """
    id_cfg = configparser.ConfigParser()
    id_cfg.read('begin_id.cfg')
    total_len = int(id_cfg[sec_name]['total_length'])
    total_len += load_length
    id_cfg.set(sec_name, "begin_id", str(max_id))
    id_cfg.set(sec_name, "post_length", str(load_length))
    id_cfg.set(sec_name, "total_length", str(total_len))
    id_cfg.write(open("begin_id.cfg", "w"))
    print('本次装入%d条,合计装入%d条' % (load_length, total_len))


def read_db_cfg():
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../db.cfg')
    return db_cfg


def read_begin_id(cfg_name):
    # 从配置文件中读取起始id
    post_cfg = configparser.ConfigParser()
    post_cfg.read('begin_id.cfg')
    begin_id = post_cfg[cfg_name]['begin_id']
    return int(begin_id)


def read_url(cfg_name):
    # 从配置文件中读取起始id
    post_cfg = configparser.ConfigParser()
    post_cfg.read('%s.cfg' % cfg_name)
    url = post_cfg[cfg_name]['url']
    return url
