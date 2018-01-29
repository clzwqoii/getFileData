import sys
import time
import pymysql
bar_length = 100
def getSchedule(progress):
    hashes = '#' * int(progress/100.0 * bar_length)
    spaces = ' ' * (bar_length - len(hashes))
    sys.stdout.write("\rPercent: [%s] %d%%"%(hashes + spaces, progress))
    sys.stdout.flush()
    time.sleep(1)

if __name__ == '__main__':
    fd = open('ipdata.txt', 'r', encoding='utf-8')
    data = fd.readlines()
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='ipdata', charset='utf8')
    # 传入保存数据的百分比到显示进度条的方法里面
    wh = True
    while wh:
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
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
