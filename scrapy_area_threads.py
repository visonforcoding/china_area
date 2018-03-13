# coding=utf-8

from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
from peewee import *
import logging
import re
import datetime
import time
import threading
'''
抓取国家统计局省市数据
'''

start_time = datetime.datetime.now()

# 创建 日志 对象
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# Connect to the database

db = MySQLDatabase('test', user='root', password='')

timeout_page = []


class BaseModel(Model):
    class Meta:
        database = db


class Area(BaseModel):
    area = CharField()
    code = CharField()
    level = IntegerField()
    pid = IntegerField()
    town_code = CharField()
    create_time = TimeField(
        default=datetime.datetime.now)

    class Meta:
        db_table = 't_area'


def scrapy_page(page_url):
    base_url = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2016/%s' % page_url

    # 伪造ua 否则无法进行
    ua = UserAgent()
    headers = {'user-agent': ua.random}
    try:
        page = requests.get(base_url, headers=headers, timeout=30)
        page_content = page.content
        return page_content
    except:
        timeout_page.append(page_url)
        return None

    # 用gb2312解码成unicode 再用utf-8 重新编码
    # page_content = page_content.decode('gb2312').encode('utf-8')
    # print page_content

def scrapy_job(thread_name, page_url,province,pid):
    logger.info('开启线程%s,处理%s' % (thread_name,province))
    # return
    sub_page_content = scrapy_page(page_url)
    if not sub_page_content:
        print('%s请求超时失败。。。' % sub_page_content)
        return
    sub_page_soup = BeautifulSoup(sub_page_content, 'html5lib')
    city_tr_tags = sub_page_soup.select('.citytr')
    for tr in city_tr_tags:
        a_tags = tr.select('td a')
        city_code = a_tags[0].text
        city_str = a_tags[1].text
        city_list.append({'code': city_code, 'area': city_str})

            ## 保存2级区域 市级
        area_2 = Area()
        area_2.level = 2
        area_2.area = city_str
        area_2.pid = pid
        area_2.code = city_code
        area_2.save()

        sub_link = a_tags[0]['href']

        sub_page_content = scrapy_page(sub_link)
        if not sub_page_content:
            print('%s请求超时失败。。。' % sub_page_content)
            continue
        sub_page_soup = BeautifulSoup(sub_page_content, 'html5lib')
        country_tr_tags = sub_page_soup.select('.countytr')

        for tr in country_tr_tags:
            a_tags = tr.select('td a')
            if not a_tags:
                continue
            country_code = a_tags[0].text
            country_str = a_tags[1].text
            city_list.append({'code': city_code, 'area': city_str})

                ## 保存3级区域 区
            area_3 = Area()
            area_3.level = 3
            area_3.area = country_str
            area_3.pid = area_2.get_id()
            area_3.code = country_code
            area_3.save()

                #处理第4级
            sub_link = a_tags[0]['href']
            reobj = re.match(r'\d*/(\d{2})\d*', sub_link)
            sub_link = reobj.group(1) + '/' + sub_link
            print(sub_link)
            sub_page_content = scrapy_page(sub_link)
            if not sub_page_content:
                print('%s请求超时失败。。。' % sub_page_content)
                continue
            sub_page_soup = BeautifulSoup(sub_page_content, 'html5lib')
            towntr_tr_tags = sub_page_soup.select('.towntr')
            print(towntr_tr_tags)
            for tr in towntr_tr_tags:
                a_tags = tr.select('td a')
                if not a_tags:
                    continue
                town_code = a_tags[0].text
                town_str = a_tags[1].text

                ## 保存4级区域 区
                area_4 = Area()
                area_4.level = 4
                area_4.area = town_str
                area_4.pid = area_3.get_id()
                area_4.code = town_code
                area_4.save()

                #处理第5级
                sub_link = a_tags[0]['href']
                reobj = re.match(r'\d*/(\d{2})(\d{2})\d*', sub_link)
                sub_link = reobj.group(1) + '/' + reobj.group(
                    2) + '/' + sub_link

                sub_page_content = scrapy_page(sub_link)
                if not sub_page_content:
                    print('%s请求超时失败。。。' % sub_page_content)
                    continue
                sub_page_soup = BeautifulSoup(sub_page_content, 'html5lib')
                village_tr_tags = sub_page_soup.select('.villagetr')

                for tr in village_tr_tags:
                    a_tags = tr.select('td')
                    if not a_tags:
                        continue
                    village_code = a_tags[0].text
                    village_town_code = a_tags[1].text
                    village_str = a_tags[2].text

                    ## 保存5级区域 居委会
                    area_5 = Area()
                    area_5.level = 5
                    area_5.area = village_str
                    area_5.pid = area_4.get_id()
                    area_5.code = village_code
                    area_5.town_code = village_town_code
                    area_5.save()
    

if __name__ == '__main__':

    #清空数据库
    db.execute_sql("truncate table t_area")

    base_page = 'index.html'

    index_content = scrapy_page(base_page)

    index_soup = BeautifulSoup(index_content, 'html5lib')

    data = []
    province_link_tags = index_soup.select('tr.provincetr td a')
    i = 0
    for link in province_link_tags:
        i = i + 1
        province = link.text
        #保存一级区域 省级
        area_1 = Area()
        area_1.level = 1
        area_1.area = province
        area_1.save()
        city_list = []
        t = threading.Thread(target=scrapy_job, args=(i, link['href'],province,area_1.get_id()))
        t.start()
        

            # print(country_tr_tags)
        # print(city_list)
        # print(data_item)
        # if i > 2:
        # break

end_time = datetime.datetime.now()

time_sub = end_time - start_time
print('花费%ss完成' % time_sub.total_seconds())
print(timeout_page)
