#!/user/bin/env python
# -*- coding:utf-8 -*-
# 作者：周桂华
# 开发时间: 2021/3/18 21:14


import pymysql
from time import perf_counter
import datetime
import getpass


# 成功次数
successcount = 0
# 失败次数
errcount = 0
# 执行次数
executecount = 0
# 总条数
allcount = 0


# 原始数据的数据连接
def ConnectSrcDB(srchost, srcuser, srcpasswd, srcdb, srctablename, srcport=3306):
    dbsrc = pymysql.connect(srchost, srcuser, srcpasswd, srcdb, srcport, charset='utf8')
    cursorsrc = dbsrc.cursor(cursor=pymysql.cursors.DictCursor)
    # 定义查询语句
    global allcount
    allcount = cursorsrc.execute("select * from {}".format(srctablename))
    dbsrc.close()
    return cursorsrc.fetchall()


# 目标数据的数据连接
def ConnectTargetDB(targethost, targetuser, targetpasswd, targetdb, targettablename, srcdatas, targetport=3306):
    dbtarget = pymysql.connect(targethost, targetuser, targetpasswd, targetdb, targetport, charset='utf8')
    cursortarget = dbtarget.cursor()
    global successcount
    global errcount
    global executecount
    global allcount
    # 定义查询语句
    start = perf_counter()
    for data in srcdatas:
        executecount += 1
        for key, value in data.items():
            if isinstance(value, datetime.datetime):
                data.update({key: value.strftime('%Y-%m-%d %H:%M:%S')})
        keys = ','.join(data.keys())
        values = ','.join(['%s'] * len(data))
        sql = "insert into %s (%s) values  (%s)" % (targettablename, keys, values)
        try:
            cursortarget.execute(sql, tuple(data.values()))
            dbtarget.commit()
            successcount += 1
        except Exception as e:
            print(e.args)
            dbtarget.rollback()
            errcount += 1
        finally:
            speed = (successcount/allcount)*100
            dur = perf_counter() - start
            print('\r进度{:^3.0f}%；源服务器：{}；源数据库：{}；目标服务器：{}；目标数据库：{}；已处理：{}；错误：{}；已传输：{}；时间：{:.2f}s'.format(
                speed, src_config_list[0], src_config_list[2], target_config_list[0], target_config_list[2], executecount, errcount, successcount, dur
            ), end='')

    dbtarget.close()

    pass


if __name__ == '__main__':
    src_config_list = input('请依次输入下面源数据库配置\n源数据库ip地址,用户名,数据库名,需要同步表名称,端口号[使用英文状态下的逗号分隔],回车键确定:\n').split(',')
    src_passwd = getpass.getpass("请输入源数据库密码,回车键确定:\n")
    target_config_list = input('请依此输入下面目标数据库配置\n目标数据库ip地址,用户名,数据库名,同步到表名称,端口号[使用英文状态下的逗号分隔],回车键确定:\n').split(',')
    target_passwd = getpass.getpass("请输入目标数据库密码,回车键确定:\n")
    res = ConnectSrcDB(src_config_list[0], src_config_list[1], src_passwd, src_config_list[2], src_config_list[3], int(src_config_list[4]))
    ConnectTargetDB(target_config_list[0], target_config_list[1], target_passwd, target_config_list[2], target_config_list[3], res, int(target_config_list[4]))


