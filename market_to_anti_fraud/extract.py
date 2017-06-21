# coding:utf-8
import pandas as pd


class Extract(object):
    """抽取原始数据类
    
    初始化需要三个对象：
        1、源数据库引擎
        2、目标数据库引擎
        3、记录完成etl情况的文件
    """
    def __init__(self,source,target,record_file):
        self.source_engine = source
        self.target_engine = target
        self.record_file = record_file

    def market(self):
        """抽取市场表
        
        :return: fact_market 表中的所有字段
        """
        try:
            with open(self.record_file, 'r') as f:
                begin_id = int(f.read())
        except FileNotFoundError as e:
            print(e)
            begin_id = 0
        except ValueError as e:
            print(e,'app2.record has no begin id !')
            begin_id = 0

        select_market = "SELECT * " \
                        "FROM fact_market " \
                        "where marketId>%s " % begin_id

        market_df = pd.read_sql_query(
            sql=select_market,
            con=self.source_engine
        )
        if len(market_df) == 0:
            raise Exception('All market samples have been done !')
        with open(self.record_file+'.tmp','w') as f:
            f.write(str(max(market_df['marketId'])))

        return market_df

    def draw_samples(self):
        """抽取绘图样本
        
        :return: 返回关于样本经营状态的数据框
        """
        sql = "SELECT " \
              " marketGuid," \
              " drawEmpty," \
              " drawRecruit," \
              " drawRenovation," \
              " drawSublease," \
              " drawWarehouse," \
              " drawNormal," \
              " drawClose " \
              "FROM fact_draw"
        return pd.read_sql_query(sql=sql,con=self.source_engine)