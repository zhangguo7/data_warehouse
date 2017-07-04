import logging


class Load(object):

    def __init__(self,target_engine):
        self.target_engine = target_engine

    def loading(self,date_table):
        try:
            date_table.to_sql(name='dimension_date',con=self.target_engine,
                                 if_exists='replace',index=False)
            logging.info('Succeed to load date_table,size = %d'%len(date_table))
        except Exception as e:
            logging.error(e)

