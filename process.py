from kafka import KafkaProducer
from json import dumps
import time
import ccxt
import os
import pymysql
import datetime
from dotenv import load_dotenv

load_dotenv()

day = 20
col = "twenty"

# db 연결
db = pymysql.connect(
    host="localhost", port=3306, user="root", passwd=os.getenv("DB_PASSWORD"), db="data", charset="utf8", autocommit=True
)
cur = db.cursor()

# 시작 시간 설정
#today = datetime.date.fromtimestamp(1504224000000)
cur.execute("select max(time) from btc_usdt_day")
ret = cur.fetchone()
end = ret[0]
cur.execute("select min(time) from btc_usdt_day")
ret = cur.fetchone()
if len(ret) == 0:
    print("No data")
    exit()
now = ret[0]

while True:
    before = now - datetime.timedelta(days = day)
    cur.execute("select close from btc_usdt_day where '"+before.strftime("%Y-%m-%d %H:%M:%S") 
        +"' < time AND time <= '"+now.strftime("%Y-%m-%d %H:%M:%S")+"'")
    ret = cur.fetchall()
    sum = 0.0
    for val in ret:
        sum += val[0]
    ins = sum/len(ret)
    cur.execute("UPDATE btc_usdt_day SET "+col+" = "+str(ins)+" WHERE time = '"+now.strftime("%Y-%m-%d %H:%M:%S")+"'")
    now += datetime.timedelta(days = 1)
    if now > end:
        print("end")
        break
