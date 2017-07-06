import configparser
import logging


class Record(object):

    def __init__(self,table,record_path):
        self.table = table
        self.record_path = record_path
        self.rec = configparser.ConfigParser()


    def get_record(self):
        """读取初始记录

        :return:启动etl的参数
        """

        self.rec.read(self.record_path)
        section = self.rec[self.table]

        max_id = int(section['max_id'])
        update_id = int(section['update_id'])
        chunksize = int(section['chunksize'])
        rounds = int((max_id - update_id) / chunksize) + 1

        start_params = {
            'max_id': max_id,
            'update_id': update_id,
            'chunksize': chunksize,
            'rounds': rounds
        }
        logging.info('start_params={max_id:%d,update_id:%d,chunksize:%d,rounds:%d}'
                     %(max_id,update_id,chunksize,rounds))
        return start_params

    def update_record(self,update_id):
        """更新记录（主要更新 update_id）

        :param update_id: 单次etl最大的一条id
        """
        try:
            self.rec.set(self.table,'update_id',str(update_id))
            self.rec.write(open(self.record_path,'w'))
        except Exception as e:
            logging.error("%s, update_id is None,record can't update"%e)
        else:
            logging.info("Succeed to update, update_id is %d" % update_id)
