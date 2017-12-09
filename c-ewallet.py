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
result = channel.queue_declare(exclusive=True)
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
		ts=msg['ts']
		ts = time.strptime(ts,'%Y-%m-%d %H:%M:%S')
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

channel.start_consuming()
