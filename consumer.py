#consumer.py
import pika, json, datetime
from peewee import *
from models import *
from playhouse.sqlite_ext import SqliteExtDatabase

database = SqliteExtDatabase('tugas2.db', journal_mode='WAL')

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
		ts= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	except Exception as e:
		print (e)
	try :
		quorum,flag = Quorum.get_or_create(npm=npm)
		quorum.timestamp = ts
		quorum.save()
	except Exception as e:
		print (e)
	database.close()

def callback(ch, method, properties, body):
	try :
		msg = json.loads(body.decode("utf-8"))
		print ("[x] ",msg)
		update_db(msg)
	except Exception as e:
		print ("[E] Error :",body, "karena ", e)

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
