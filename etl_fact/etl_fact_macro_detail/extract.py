import logging

import pandas as pd


class Extract(object):

    def __init__(self,source_target,target_engine):
        self.source_target = source_target
        self.target_engine = target_engine

    def get_start_params(self,chunksize):

        return

    def tag_details(self,start_id,end_id):
        """从抓取数据库里抽取宏观信息

        :return:宏观信息的dataframe
        """
        sql = "SELECT *" \
              " FROM tag_detail" \
              " WHERE id BETWEEN %d AND %d"%(start_id,end_id)
        tag_details = pd.read_sql_query(sql=sql,con=self.source_target)
        logging.info('Succeed to extract tag details,size=%d'%len(tag_details))

        return tag_details

    def std_divisions(self):
        """提取标准的division表"""
        divisions = pd.read_sql_table(
            table_name='dimension_division',
            con=self.target_engine,
            columns=['divisionKey','divisionProv','divisionCity','divisionDistrict']
        )
        assert len(divisions) > 2000

        logging.info('Succeed to extract divisions')

        return divisions