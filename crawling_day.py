#!python3
import time
import ccxt
import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

db = pymysql.connect(
    host="localhost", port=3306, user="root", passwd=os.getenv('DB_PASSWORD'), db="data", charset="utf8", autocommit=True
)

cur = db.cursor()


def table_check(table_name):
    cur.execute("show tables like '" + table_name + "'")

    if len(cur.fetchall()) == 0:
        cur.execute(
            "create table "
            + table_name
            + "(time TIMESTAMP NOT NULL, "
            + "open FLOAT(32, 16) NOT NULL, "
            + "high FLOAT(32, 16) NOT NULL, "
            + "low FLOAT(32, 16) NOT NULL, "
            + "close FLOAT(32, 16) NOT NULL, "
            + "volume FLOAT(32, 16) NOT NULL, "
            + "PRIMARY KEY (time))"
        )
        db.commit()


def find_last(table_name, or_this):
    cur.execute("select * from " + table_name)
    val = cur.fetchall()

    if len(val) == 0:
        return or_this

    cur.execute("select max(time) from " + table_name)
    val = cur.fetchall()
    print(val[0][0], len(val))
    print(time.mktime(val[0][0].timetuple()))

    return int(time.mktime(val[0][0].timetuple()) * 1000)


def main():
    certified_list = [
        "binance",
        "bitfinex",
        "bittrex",
        "bitvavo",
        "bytetrade",
        "eterbase",
        "ftx",
        "idex",
        "kraken",
        "poloniex",
        "upbit",
    ]
    binance = ccxt.binance(
        {
            "apiKey": "25DBy3mRTW81NH1jTbJOVF4nSJLdr5G0ncAEjKIOQd1az44wETMOPaLQmlYSmSsF",
            "secret": "Ymfbvn7xLJsHG7sYgsPRtyJwkNCGilhoTIMEzIPkZ8dxY5ZU7ZL8mrz7nW9j3wZM",
        }
    )

    # val = binance.load_markets()
    # for i in val:
    #     print(i)

    pair_name = "btc_usdt_day"

    table_check(pair_name)
    start = find_last(pair_name, 1504224000000)  # 2017/08/15T09:00:00

    while True:
        val = binance.fetch_ohlcv("BTC/USDT", timeframe="1d", since=start, limit=1000)

        print(start, len(val))

        time.sleep(binance.rateLimit / 1000)

        for i in val:
            sql = (
                "INSERT INTO "
                + pair_name
                + "(time, open, high, low, close, volume) SELECT FROM_UNIXTIME("
                + str(i[0] / 1000)
                + "), "
                + str(i[1])
                + ", "
                + str(i[2])
                + ", "
                + str(i[3])
                + ", "
                + str(i[4])
                + ", "
                + str(i[5])
                + " WHERE NOT EXISTS (SELECT * FROM "
                + pair_name
                + " WHERE time = FROM_UNIXTIME("
                + str(i[0] / 1000)
                + "));"
            )

            cur.execute(sql)

        db.commit()

        if len(val) == 0:
            start += 24*60*60*1000
        else:
            start = val[-1][0] + 24*60*60*1000

        if start > int(time.time())*1000:
            break



main()
