import logging
import logging.config

logging.config.fileConfig('logs.cfg')


def log_info(func):
    """info 级别的log装饰器"""
    def wrap(*args, **kwargs):
        logging.info('Start %s: args - %s, kwargs - %s' % (func.__name__, args, kwargs))
        return func(*args, **kwargs)

    return wrap


def log_warning(func):
    """warning 级别的log装饰器"""
    def wrap(*args,**kwargs):
        logging.warning('Start %s: args - %s, kwargs - %s' % (func.__name__, args, kwargs))
        return func(*args,**kwargs)

    return wrap


@log_info
def other2int(x):
    try:
        return int(x)
    except:
        print(x,'can not be transform to int')


other2int('info')


@log_warning
def other2int(x):
    try:
        return int(x)
    except:
        print(x,'can not be transform to int')
other2int('warning')