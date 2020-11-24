# -*- coding: utf8 -*-
import os
import signal
import pymysql
from dotenv import load_dotenv

from RestrictedPython import compile_restricted
from RestrictedPython import Eval
from RestrictedPython import utility_builtins
from RestrictedPython.PrintCollector import PrintCollector
from RestrictedPython.Guards import full_write_guard

from kafka import KafkaConsumer
from json import loads, dumps

### time out
class TimeOutException(Exception):
	pass

def alarm_handler(signum, frame):
	raise TimeOutException()

def getitem(obj, index):
	if obj is not None and type(obj) in (list, tuple, dict):
		return obj[index]
	raise Exception()

def main():
	# load env
	load_dotenv()

	# make db connection
	db_pw = os.getenv("DB_PASSWORD")

	logic_db = pymysql.connect(user="root", passwd=db_pw, host="127.0.0.1", db="algotrade", charset="utf8", autocommit=True)

	cursor = logic_db.cursor(pymysql.cursors.DictCursor)


	return_variable_function = """
def returnGlobalVariableWiquant():
  return variable
"""

	# 카프카 서버
	bootstrap_servers = ["localhost:9092"]

	# 카프카 토픽
	str_topic_name = "Topic1"

	# 카프카 소비자 group1 생성
	str_group_name = "group1"

	consumer = KafkaConsumer(
		str_topic_name,
		bootstrap_servers=bootstrap_servers,
		auto_offset_reset="earliest",  # 가장 처음부터 소비
		enable_auto_commit=True,
		group_id=str_group_name,
		value_deserializer=lambda x: loads(x.decode("utf-8")),
	)

	for message in consumer:
		value = message[6]
		print(value['yesterday'])
		print(value['today'])
		print(value['moving_average'])
		print(type(value['moving_average']))

		sql = "SELECT * FROM logic;"
		cursor.execute(sql)
		logics = cursor.fetchall()
		print("logic list", logics)
		for logic in logics:
			print(logic['content'])
			print(logic['variable'])

			# config and inject data and function
			global_builtins = {"__builtins__": utility_builtins}.copy()
			global_builtins["_getiter_"] = Eval.default_guarded_getiter
			global_builtins["_getattr_"] = getattr
			global_builtins["_setattr_"] = setattr
			global_builtins["_getitem_"] = getitem
			global_builtins["_print_"] = PrintCollector
			global_builtins["_write_"] = full_write_guard

			global_builtins["my_wallet"] = {'usdt':500000, 'btc':0.1}
			global_builtins["yesterday"] = value['yesterday']
			global_builtins["today"] = value['today']
			global_builtins["moving_average"] = value['moving_average']


			if not logic['variable']:
				variable = {}
			else:
				variable = loads(logic['variable'])
			global_builtins["variable"] = variable

			byte_code = compile_restricted(logic["content"]+return_variable_function, "<inline code>", "exec", None)

			signal.signal(signal.SIGALRM, alarm_handler)
			signal.alarm(1)

			loc = {}

			try:
				exec(byte_code, {"__builtins__": global_builtins}, loc)
				trade = loc["logic"]()

				print('buy or sell:', trade)
				variable2db = loc['returnGlobalVariableWiquant']()
				print('update variable', variable2db)
				var = dumps(variable2db)
				id = logic['id']
				print(var, id)
				sql = "update logic set variable='"+var+"' where id="+str(id)+";"
				print(sql)
				cursor.execute(sql)

			except TimeOutException as e:
				print(e)

			signal.alarm(0)

main()
