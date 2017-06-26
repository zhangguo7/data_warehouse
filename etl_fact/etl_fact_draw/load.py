# -*- coding:utf-8 -*-


class Load(object):
    """将转换结果装载到数据仓库

    由load_main实现"""
    def __init__(self, engine):
        self.engine = engine

    def load_main(self, df):
        df.to_sql(name='fact_draw', con=self.engine, if_exists='append', index=False)