from oslo.config import cfg
from stevedore import driver

OPTS=[
    cfg.StrOpt('driver',
               default='moving-average')
]
cfg.CONF.register_opts(OPTS, group='aggregates')

def __get_driver(name, conf):
    d = driver.DriverManager('gnocchi.aggregates',
                             name).driver
    return d(conf)

def get_driver(conf):
    return __get_driver(conf.aggregates.driver,
                            conf.aggregates)

def get_name(conf):
    return conf.aggregates

class StatisticsDriver(object):

    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def compute(data, **params):
        raise NotImplementedError
