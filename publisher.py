# publisher.py
import pika, json, datetime, time

params = pika.URLParameters('amqp://sisdis:sisdis@172.17.0.3:5672')
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange='EX_PING',exchange_type='fanout')
while True:
	msg = {}
	msg['action']= 'ping'
	msg['npm']= '1406559055'
	msg['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
	msg = json.dumps(msg)
	channel.basic_publish(exchange='EX_PING',routing_key='',body=msg)
	print ("[x] Message sent to consumer")
	time.sleep(5)
