import configparser

db_cfg = configparser.ConfigParser()
db_cfg.read('db.cfg')
if not db_cfg['dw'].get('port'):
    print('with port')
else:
    print('1')