#coding=utf-8
'''
    主要目的： 将 ipdata.txt 中的数据导入到我们的数据库中
    author ; zhyh2010 create in 2016.05.30
'''


import pymysql
import sys
import time
from threading import Thread
bar_length = 100
allItems = []

class Ipdata:
    '''
    将 ipdata.txt 中的数据导入到我们的数据库中

    主要分为两个模块， 数据库操作， 文件操作
    '''
    def __init__(self):
        print('程序初始化中...')
        self.DROP_TABLE = 'drop table if exists ipdata;'
        self.CREATE_TABLE = ' create table ipdata( \
                            id bigint(20) not null auto_increment primary key, \
                            start_ip varchar(45) default null, \
                            end_ip varchar(45) default null, \
                            country varchar(45) default null, \
                            local varchar(300) default null \
                        ) charset = utf8;'
        self.sql_model = 'insert into `ipdata` (`start_ip`, `end_ip`, `country`, `local`) values(%s, %s, %s, %s)'
        self.values = []
        self.filename = "ipdata.txt"
        self.filecode = "utf-8"

    def async(f):
        def wrapper(*args, **kwargs):
            thr = Thread(target=f, args=args, kwargs=kwargs)
            thr.start()
        return wrapper

    def LoadData(self):
        print('读取文件数据中......')
        # load data from ipdata.txt
        count = 1
        fd = open(self.filename, 'r', encoding='utf-8')
        data = fd.readlines()
        for k, line in enumerate(data):
            ratio = float((k+1) / len(data) * 100)
            #显示进度条
            progress(ratio)
            line = line.encode().decode(self.filecode, 'ignore')
            items = line.split()
            items_new = []
            string = ' '
            if(len(items) < 4):
                items.append('')
            else:
                items[3] = string.join(items[3:])
                del items[4:]
            for key, item in enumerate(items):
                items_new.append(item)
            value = tuple(items_new)
            count = count + 1
            self.values.append(value)
        print('')
        print('共' + repr(count) + '条数据')
        # print('共' + str(count) + '条数据')
        # self.values = self.values[1:]
        # self.values = self.values

    @async
    def InsertIntoDB(self):
        # connect to the db and insert data
        self.LoadData()
        print('..............')
        conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='ipdata', charset='utf8')
        # cursor = conn.cursor()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(self.DROP_TABLE)
        cursor.execute(self.CREATE_TABLE)
        cursor.executemany(self.sql_model, self.values)
        # cursor.execute(self.sql_model, ('1.0.1.0', '1.0.3.255', 'dsds', '士大夫撒旦'))
        conn.commit()
        conn.close()

def runSchedule():
    fd = open('ipdata.txt', 'r', encoding='utf-8')
    data = fd.readlines()
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='ipdata', charset='utf8')
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    # 传入保存数据的百分比到显示进度条的方法里面
    wh = True
    while wh:
        cursor.execute("select max(id) as maxId from ipdata")
        res = cursor.fetchone()
        if res['maxId']:
            maxId = res['maxId']
        else:
            maxId = 0
        lv = maxId / len(data) * 100
        getSchedule(lv)
        if lv == 100:
            wh = False

def getSchedule(progress):
    hashes = '#' * int(progress/100.0 * bar_length)
    spaces = ' ' * (bar_length - len(hashes))
    sys.stdout.write("\rPercent: [%s] %d%%"%(hashes + spaces, progress))
    sys.stdout.flush()
    time.sleep(1)

def progress(percentage):
    # time.sleep(0.00001)
    hashes = '#' * int(percentage/100.0 * bar_length)
    spaces = ' ' * (bar_length - len(hashes))
    sys.stdout.write("\r Progress: [%s] %d%%"%(hashes + spaces, percentage))
    sys.stdout.flush()
    # for percent in range(0, 100):
    #     hashes = '#' * int(percent / 100.0 * bar_length)
    #     spaces = ' ' * (bar_length - len(hashes))
    #     sys.stdout.write("\rPercent: [%s] %d%%" % (hashes + spaces, percent))
    #     sys.stdout.flush()
    #     time.sleep(1)

if __name__ == "__main__":
    print('启动')
    ipdata = Ipdata()
    ipdata.InsertIntoDB()
    time.sleep(10)
    runSchedule()
