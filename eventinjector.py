from kafka import KafkaProducer
from json import dumps
import time
import ccxt
import os
import pymysql
import datetime
from dotenv import load_dotenv

def on_send_success(record_metadata):
    # 보낸데이터의 매타데이터를 출력한다
    print("record_metadata:", record_metadata)


load_dotenv()

# 카프카 서버
bootstrap_servers = ["localhost:9092"]

# 카프카 공급자 생성
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         key_serializer=None,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# 카프카 토픽
str_topic_name = 'Topic1'

# db 연결
db = pymysql.connect(
    host="localhost", port=3306, user="root", passwd=os.getenv('DB_PASSWORD'), db="data", charset="utf8", autocommit=True
)
cur = db.cursor()

# 시작 시간 설정
#today = datetime.date.fromtimestamp(1504224000000)
cur.execute("select min(time) from btc_usdt_day")
ret = cur.fetchone()
if len(ret) == 0:
    print("No data")
    exit()
now = ret[0]

while True:
    cur.execute("select high from btc_usdt_day where time='"+now.strftime("%Y-%m-%d %H:%M:%S")+"'")
    ret = cur.fetchone()

    time.sleep(10)
    if len(ret) == 0:
        continue

    price = ret[0]
    # 카프카 공급자 토픽에 데이터를 보낸다
    data = {"value": price, 
		"yesterday": {'open':19144, 'high':19160, 'low':19000, 'close':19100, 'volume':0.02},
		"today": {'open':19144, 'high':19360, 'low':19100, 'close':19300, 'volume':0.10},
		"moving_average": {'5':19100, '10':19000, '20':18900}
	}

    producer.send(str_topic_name, value=data).add_callback(on_send_success)\
                                         .get(timeout=10) # blocking maximum timeout
    print('data:', data)

    now += datetime.timedelta(days = 1)

