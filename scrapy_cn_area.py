# coding=utf-8

from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
from peewee import *
import logging
import datetime
import time
import threading
import coloredlogs
from pymongo import MongoClient
import hashlib
import base64

# 菜鸟 地址 查询接口

# 创建 日志 对象
coloredlogs.install()
# 创建 日志 对象
logger = logging.getLogger('peewee')
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logging.info('开始抓取菜鸟4级区域信息')

app_key = '63cetR665147l40145u4hc6g0w7p3Q89'
logistic_provider_id = '4ca6d53fa73a00494181458ddf55e2ff'

# Connect to the mongodb database

mongoconn = MongoClient('192.168.33.10', 27017)
mdb = mongoconn.geoinfo
pois = mdb.pois

# Connect to the mysql

db = MySQLDatabase('test', user='root', password='')


class BaseModel(Model):
    class Meta:
        database = db


class Area(BaseModel):
    division_id = IntegerField()
    parent_id = IntegerField()
    division_level = IntegerField()
    division_name = CharField()
    division_name_pinyin = CharField()
    division_tname = CharField()
    division_abb_name = CharField()
    is_deleted = IntegerField()
    create_time = TimeField(default=datetime.datetime.now)

    class Meta:
        db_table = 't_division'


def signData(inputs):
    """[签名数据]
    
    Arguments:
        inputs {[str]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    m1 = hashlib.md5()
    m1.update((inputs + app_key).encode('utf-8'))
    return base64.b64encode(m1.digest())


def reqPlace(data):
    """[summary]
    
    Arguments:
        data {[dict]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    url = 'http://link.cainiao.com/gateway/link.do'
    data_digest = signData(str(data))
    postdata = {
        'msg_type': 'CNDZK_CHINA_SUB_DIVISIONS',
        'logistic_provider_id': logistic_provider_id,
        'data_digest': data_digest,
        'logistics_interface': str(data)
    }
    res = requests.post(url, postdata)
    return res.json()


def save_division(obj):
    try:
        area = Area.get(Area.division_id == obj['divisionId'])
        logging.info('%s存在', obj['divisionTname'])
        return None
    except Exception as e:
        logging.info('%s不存在', obj['divisionTname'])
    area = Area()
    area.division_id = obj['divisionId']
    area.parent_id = obj['parentId']
    area.division_level = obj['divisionLevel']
    area.division_abb_name = obj['divisionAbbName']
    area.division_name_pinyin = obj['pinyin']
    area.division_name = obj['divisionName']
    area.division_tname = obj['divisionTname']
    area.is_deleted = 0
    if obj['isdeleted'] != 'false':
        area.is_deleted = 1
    try:
        area.save()
        return area.get_id()
    except Exception as e:
        logging.error('save数据时发生了异常:%s,异常原因%s' % (obj['divisionTname'], e))
        print(obj)
        return None


def thread_job(item, i):
    print(item)
    res = reqPlace({'divisionId': item['divisionId']})
    if res['success'] == 'true':
        for item in res['divisionsList']:
            save_res = save_division(item)
            if save_res:
                logging.info('线程%i保存一条记录' % i)
            thread_job(item, i)


def hand_thread(i, item):
    logging.info('线程%s开启' % i)
    thread_job(item, i)


if __name__ == '__main__':
    res = reqPlace({'divisionId': 1})
    i = 0
    if res['success'] == 'true':
        for item in res['divisionsList']:
            i = i + 1
            save_division(item)
            t = threading.Thread(target=hand_thread, args=(i, item))
            t.start()