#consumer.py
import pika, json, datetime, time
from peewee import *
from models import *
from playhouse.sqlite_ext import SqliteExtDatabase

database = SqliteExtDatabase('tugas2.db', journal_mode='WAL')

params = pika.URLParameters('amqp://sisdis:sisdis@172.17.0.3:5672')
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange='EX_REGISTER', exchange_type='direct',durable=True)
result = channel.queue_declare()
queue_name = result.method.queue
channel.queue_bind(exchange='EX_REGISTER',queue=queue_name,routing_key='REQ_1406559055')
print ('[X] Waiting for logs')
def register(msg):
	database.connect()
	resp = {}
	resp['action'] = 'register'
	resp['type'] = 'response'
	resp['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	try :
		user_id = msg['user_id']
		sender_id = msg['sender_id']
		nama =msg['nama']
	except Exception as e:
		print ("[E] Error :",e)
		resp['status_register'] = -99
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_REGISTER',routing_key='RESP_'+sender_id,body=resp)
	try :
		User.create(name=nama,npm=user_id)
		database.close()
	except Exception as e:
		print ("[E] Error :",e)
		resp['status_register'] = -4
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_REGISTER',routing_key='RESP_'+sender_id,body=resp)
	resp['status_register'] = 1
	resp = json.dumps(resp)
	return channel.basic_publish(exchange='EX_REGISTER',routing_key='RESP_'+sender_id,body=resp)

def request_register(ch, method, properties, body):
	try :
		msg = json.loads(body.decode("utf-8"))
		print ("[x] ",msg['action']," message :",msg)
		register(msg)
	except Exception as e:
		print ("[E] Error :",e)

channel.basic_consume(request_register, queue=queue_name, no_ack=True)

channel.exchange_declare(exchange='EX_TRANSFER', exchange_type='direct',durable=True)
result = channel.queue_declare()
queue_name = result.method.queue
channel.queue_bind(exchange='EX_TRANSFER',queue=queue_name,routing_key='REQ_1406559055')
def transfer(msg):
	database.connect()
	resp = {}
	resp['action'] = 'transfer'
	resp['type'] = 'response'
	resp['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	try :
		user_id = msg['user_id']
		sender_id = msg['sender_id']
		nilai = msg['nilai']
	except Exception as e:
		print ("[E] Error :",e)
		resp['status_transfer'] = -99
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_TRANSFER',routing_key='RESP_'+sender_id,body=resp)
	try :
		user = User.get(npm=user_id)
	except Exception as e:
		print ("[E] Error :",e)
		resp['status_transfer'] = -1
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_TRANSFER',routing_key='RESP_'+sender_id,body=resp)
	if nilai < 0 or nilai > 1000000000 :
		print ("[E] Error :",e)
		resp['status_transfer'] = -5
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_TRANSFER',routing_key='RESP_'+sender_id,body=resp)
	try :
		user.saldo += nilai
		user.save()
	except Exception as e :
		print ("[E] Error :",e)
		resp['status_transfer'] = -4
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_TRANSFER',routing_key='RESP_'+sender_id,body=resp)
	resp['status_transfer'] = 1
	resp = json.dumps(resp)
	return channel.basic_publish(exchange='EX_TRANSFER',routing_key='RESP_'+sender_id,body=resp)


def request_transfer(ch, method, properties, body):
	try:
		msg = json.loads(body.decode("utf-8"))
		print ("[x] ",msg['action']," message :",msg)
		transfer(msg)
	except Exception as e:
		print ("[E] Error :",e)

channel.basic_consume(request_transfer, queue=queue_name, no_ack=True)

channel.exchange_declare(exchange='EX_GET_SALDO', exchange_type='direct',durable=True)
result = channel.queue_declare()
queue_name = result.method.queue
channel.queue_bind(exchange='EX_GET_SALDO',queue=queue_name,routing_key='REQ_1406559055')
def get_saldo(msg):
	database.connect()
	resp = {}
	resp['action'] = 'get_saldo'
	resp['type'] = 'response'
	resp['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	try :
		user_id = msg['user_id']
		sender_id = msg['sender_id']
	except Exception as e:
		print ("[E] Error :",e)
		resp['nilai_saldo'] = -99
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_GET_SALDO',routing_key='RESP_'+sender_id,body=resp)
	try:
		user = User.get(npm=user_id)
	except Exception as e:
		print ("[E] Error :",e)
		resp['nilai_saldo'] = -1
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_GET_SALDO',routing_key='RESP_'+sender_id,body=resp)
	try:
		resp['nilai_saldo'] = user.saldo
	except Exception as e:
			print ("[E] Error :",e)
			resp['nilai_saldo'] = -4
			resp = json.dumps(resp)
			return channel.basic_publish(exchange='EX_GET_SALDO',routing_key='RESP_'+sender_id,body=resp)
	resp = json.dumps(resp)
	return channel.basic_publish(exchange='EX_GET_SALDO',routing_key='RESP_'+sender_id,body=resp)


def request_get_saldo(ch, method, properties, body):
	try:
		msg = json.loads(body.decode("utf-8"))
		print ("[x] ",msg['action']," message :",msg)
		get_saldo(msg)
	except Exception as e:
		print ("[E] Error :",e)

channel.basic_consume(request_get_saldo, queue=queue_name, no_ack=True)

channel.start_consuming()
