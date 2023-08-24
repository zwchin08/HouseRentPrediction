#  重写getHouseInfo将info字典中的数据保存到scv文件中
# 使用pandas，将字典转换成字段数组

import pandas as pd
import time
import requests as req
from bs4 import BeautifulSoup


def getHouseInfo(url):
    info = {}
    soup = BeautifulSoup(req.get(url).text, "html.parser")
    # res1 賃料" を取って
    res1 = soup.select(".property_view_note-list span.property_view_note-emphasis  ")[0].text
    res2 = soup.select(".property_view_table td ")

    # 获取 '所在地', '間取り', '築年数', '向き', '賃料'
    info["間取り"] = res2[-6].text
    info["専有面積"] = res2[-5].text
    info["築年数"] = res2[-4].text
    info["階"] = res2[-3].text
    info["向き"] = res2[-2].text
    info["賃料"] = res1
    return info


import requests as requests
from bs4 import BeautifulSoup

# 获取详情信息跳转的域名
domain = "https://suumo.jp"


def pageFun(i):
    page = "page=" + i
    page_url = domain + "/jj/chintai/ichiran/FR301FC001/?ar=010&bs=040&pc=20&smk=&po1=25&po2=99&shkr1=03&shkr2=03&shkr3=03&shkr4=03&ta=01&sa=01&cb=0.0&ct=9999999&ts=3&et=9999999&mb=0&mt=9999999&cn=9999999&fw2=&" + page
    print(page_url)
    res = req.get(page_url)
    print(res)
    soup = BeautifulSoup(res.text, "html.parser")
    houses = soup.select(".cassetteitem")
    page_info_list = []

    # 遍历返回一页的房屋信息
    for house in houses:
        # 异常处理
        try:
            # 这里边有个问题  不知道为什么class里边有两个地址
            info = getHouseInfo(domain + house.select(".ui-text--midium   a")[0]["href"])
            page_info_list.append(info)
            #             print(domain + house.select(".ui-text--midium   a")[0]["href"])
            # 因为很多网站有反扒机制，如果持续不断地去爬取，会触发网站的反扒机制会锁掉你的IP让你无法访问
            # 多くのウェブサイトにはスクレイピング防止の仕組みがあります。継続的にスクレイピングを行うと、
            # ウェブサイトの防御機能が作動し、IPがブロックされてアクセスできなくなる可能性があります。#
            time.sleep(0.5)
        except Exception as e:
            print("--------------->", e)

    df = pd.DataFrame(page_info_list)
    # ウェブサイトからスクレイピングしたデータを"houseinfo.csv"ファイルに追加する。
    df.to_csv("houseinfo.csv")
    return df


df = pd.DataFrame()
name_prefix = "houseinfo_"

from sqlalchemy import create_engine

yconnect = create_engine('mysql://root:hsp@localhost:3306/spider?charset=utf8')

for i in range(1, 36):

    try:
        df_a = pageFun(str(i))
    except Exception as e:
        print("Exception:", e)
    pd.io.sql.to_sql(df_a, 'house_price3', yconnect, schema='spider', if_exists='append')
