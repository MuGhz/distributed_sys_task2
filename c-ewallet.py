#consumer.py
import pika, json, datetime, time
from peewee import *
from models import *
from playhouse.sqlite_ext import SqliteExtDatabase

database = SqliteExtDatabase('tugas2.db', journal_mode='WAL')
database.connect()
params = pika.URLParameters('amqp://sisdis:sisdis@172.17.0.3:5672')
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange='EX_REGISTER', exchange_type='direct',durable=True)
result = channel.queue_declare()
queue_name = result.method.queue
channel.queue_bind(exchange='EX_REGISTER',queue=queue_name,routing_key='REQ_1406559055')
print ('[X] Waiting for logs')
def count_quorum():
	q = Quorum.select()
	now = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	x = [z for z in q if now - q.ts < 10]
	result = (len(x)/len(q)) * 100
	print ("Hasil Quorum :",result)
	return (result,q)

def register(msg):
	resp = {}
	resp['action'] = 'register'
	resp['type'] = 'response'
	resp['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	try :
		result,q = count_quorum()
		if (result < 50) :
			resp['status_register'] = -2
			resp = json.dumps(resp)
			return channel.basic_publish(exchange='EX_REGISTER',routing_key='RESP_'+sender_id,body=resp)
	except Exception as e:
		print ("[E] Error :",e)
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
		resp['status_register'] = 1
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_REGISTER',routing_key='RESP_'+sender_id,body=resp)
	except Exception as e:
		print ("[E] Error :",e)
		resp['status_register'] = -4
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
	resp = {}
	resp['action'] = 'transfer'
	resp['type'] = 'response'
	resp['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	try :
		result,q = count_quorum()
		if (result < 50) :
			resp['status_transfer'] = -2
			resp = json.dumps(resp)
			return channel.basic_publish(exchange='EX_TRANSFER',routing_key='RESP_'+sender_id,body=resp)
	except Exception as e:
		print ("[E] Error :",e)
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
	resp = {}
	resp['action'] = 'get_saldo'
	resp['type'] = 'response'
	resp['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	try :
		result,q = count_quorum()
		if (result < 50) :
			resp['nilai_saldo'] = -2
			resp = json.dumps(resp)
			return channel.basic_publish(exchange='EX_GET_SALDO',routing_key='RESP_'+sender_id,body=resp)
	except Exception as e:
		print ("[E] Error :",e)
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

channel.exchange_declare(exchange='EX_GET_TOTAL_SALDO', exchange_type='direct',durable=True)
result = channel.queue_declare()
queue_name = result.method.queue
channel.queue_bind(exchange='EX_GET_TOTAL_SALDO',queue=queue_name,routing_key='REQ_1406559055')
def get_total_saldo(msg):
	resp = {}
	resp['action'] = 'get_total_saldo'
	resp['type'] = 'response'
	resp['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	try :
		result,q = count_quorum()
		if (result < 100) :
			resp['status_register'] = -2
			resp = json.dumps(resp)
			return channel.basic_publish(exchange='EX_GET_TOTAL_SALDO',routing_key='RESP_'+sender_id,body=resp)
	except Exception as e:
		print ("[E] Error :",e)
	try :
		user_id = msg['user_id']
		sender_id = msg['sender_id']
	except Exception as e:
		print ("[E] Error :",e)
		resp['nilai_saldo'] = -99
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_GET_TOTAL_SALDO',routing_key='RESP_'+sender_id,body=resp)
	try:
		users = Quorum.select()
	except Exception as e:
		print ("[E] Error:",e)
		resp['nilai_saldo'] = -4
		resp = json.dumps(resp)
		return channel.basic_publish(exchange='EX_GET_TOTAL_SALDO',routing_key='RESP_'+sender_id,body=resp)

	channel.exchange_declare(exchange='EX_GET_SALDO',exchange_type='direct',durable=True)
	result = channel.queue_declare()
	queue_name = result.method.queue
	channel.queue_bind(exchange='EX_GET_SALDO',queue=queue_name,routing_key="RESP_1406559055")
	#users = ['1406577386',  # nanda
	#'1406559036', #gales
    #    '1406559055'  # ghozi
	#]
	users = [x for x in q if x.npm != '1406559055']
	print (users)
	for user in users:
		msg = {}
		msg['action'] = 'get_saldo'
		msg['user_id'] = user_id
		msg['sender_id'] = '1406559055'
		msg['type'] = 'request'
		msg['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
		msg = json.dumps(msg)
		channel.basic_publish(exchange='EX_GET_SALDO',routing_key='REQ_'+user.npm,body=msg)
	total = 0
	n = len(users)
	i = 0
	print ("start poll")
	while i < n:
		method,properties,body = channel.basic_get(queue=queue_name)
		print (body)
		try:
			res = json.loads(body.decode("utf-8"))
		except Exception as e:
			print ("[E] Error :",e)
		try :
			saldo = res['nilai_saldo']
			if saldo > 0 :
				total += saldo
			elif saldo < 0 :
				total = saldo
				break
			i += 1
		except Exception as e:
			print ("[E] Error :",e)
	total += User.get(npm=user_id).saldo
	print ("finish poll")
	print ("total :",total," sender :",sender_id)
	resp['sender_id'] = sender_id
	resp['nilai_saldo'] = total
	resp= json.dumps(resp)
	return channel.basic_publish(exchange='EX_GET_TOTAL_SALDO',routing_key='RESP_'+sender_id,body=resp)

def request_get_total_saldo(ch, method, properties, body):
	try:
		msg = json.loads(body.decode("utf-8"))
		print ("[x] ",msg['action']," message :",msg)
		get_total_saldo(msg)
	except Exception as e:
		print ("[E] Error :",e)
channel.basic_consume(request_get_total_saldo, queue=queue_name, no_ack=True)

channel.start_consuming()
database.close()
