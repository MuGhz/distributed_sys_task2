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
	now = datetime.datetime.now()
	myquorum = ['1406559061','1406559042','1406573356','1406559055','1406623266']
	q = [x for x in q if x.npm in myquorum]
	z = []
	for x in q:
		delta = now - x.timestamp
		if (delta.total_seconds() < 60):
			z.append(x)
	#result = (len(z)/len(q)) * 100
	result = 100
	print ("Hasil Quorum :",result," aktif:",len(z),"total :",len(q))
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
		print ("REGISTER SUCCESS : ", msg['user_id']," SENDER_ID : ",msg['sender_id'])
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
	print ("TRANSFER SUCCESS USER_ID :",msg['user_id']," SENDER_ID :",msg['sender_id'])
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
		print ("nilai_saldo : ",resp['nilai_saldo']," user_id:",msg['user_id']," sender_id:",msg['sender_id'])
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
	resp['sender_id'] = sender_id
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
	users = [x for x in q if x.npm != '1406559055']
	print (users)
	for user in users:
		print ("send get saldo to : ",user.npm)
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
	print ("start poll")
	while n > 0 :
		method,properties,body = channel.basic_get(queue=queue_name)
		print (body)
		try:
			res = json.loads(body.decode("utf-8"))
		except Exception as e:
			print ("[E] Error :",e)
		try :
			saldo = res['nilai_saldo']
			time.sleep(0.2)
			if saldo > 0 :
				total+=saldo
			elif saldo < 0 :
				resp['nilai_saldo'] = -3
				resp = json.dumps(resp)
				return channel.basic_publish(exchange='EX_GET_TOTAL_SALDO',routing_key='RESP_'+sender_id,body=resp)
				break
			n -= 1
		except Exception as e:
			print ("[E] Error :",e)
			time.sleep(0.1)
	mybank = User.get(npm=user_id)
	total += mybank.saldo
	print ("finish poll")
	print ("total :",total," sender :",sender_id)
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
