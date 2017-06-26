# -*- coding:utf-8 -*-

class Load(object):

    def __init__(self,engine_target):
        self.engine_target = engine_target

    def loading(self,clean):
        clean.to_sql(name='fact_market',
                     con=self.engine_target,
                     if_exists='append',
                     index=False)


