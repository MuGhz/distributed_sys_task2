#consumer.py
import pika, json
from peewee import *
from models import *

database = SqliteDatabase('tugas2.db')

params = pika.URLParameters('amqp://sisdis:sisdis@172.17.0.3:5672')
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.exchange_declare(exchange='EX_PING', exchange_type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='EX_PING',queue=queue_name)
print(' [*] Waiting for logs. To exit press CTRL+C')

def update_db(msg):
	database.connect()
	try :
		npm =msg['npm']
		ts=msg['ts']
	except Exception as e:
		print (e) 
	try :
		user,flag = User.get_or_create(npm=npm)
		user.ts = ts
		user.save()
	except Exception as e:
		print (e)
	database.close()

def callback(ch, method, properties, body):
	msg = json.loads(body.decode("utf-8"))
	print ("[x] ",msg)
	update_db(msg)

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
