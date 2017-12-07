# ewallet.py
import pika, json, datetime, time

params = pika.URLParameters('amqp://sisdis:sisdis@172.17.0.3:5672')
connection = pika.BlockingConnection(params)
channel = ''
n=input()
arg=n.split(" ")

def response_register(ch, method, properties, body):
    try :
        msg = json.loads(body.decode("utf-8"))
    except Exception as e:
        print ("[E] Receive :",body)
        print ("[E] Error :",e)
    try :
        print ("status_register = ",msg['status_register'])
    except Exception as e:
        print ("[E] Error :",e)
    channel.close()

def register(user_id,nama,req_id):
    channel = connection.channel()
    channel.exchange_declare(exchange='EX_REGISTER',exchange_type='direct',durable=True)
    msg = {}
    msg['action']= 'register'
    msg['user_id']= user_id
    msg['nama'] = nama
    msg['sender_id'] = '1406559055'
    msg['type'] = 'request'
    msg['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
    msg = json.dumps(msg)
    channel.basic_publish(exchange='EX_REGISTER',routing_key='REQ_'+req_id,body=msg)
    print ("[x] published")
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='EX_REGISTER',queue=queue_name,routing_key='RESP_1406559055')
    channel.basic_consume(response_register, queue=queue_name, no_ack=True)
    print ("waiting response")
    channel.start_consuming()



def saldo(user_id,req_id):
    channel = connection.channel()
    channel.exchange_declare(exchange='EX_GET_SALDO',exchange_type='direct',durable=True)
    msg = {}
    msg['action'] = 'get_saldo'
    msg['user_id'] = user_id
    msg['sender_id'] = '1406559055'
    msg['type'] = 'request'
    msg['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
    msg = json.dumps(msg)
    channel.basic_publish(exchange='EX_GET_SALDO',routing_key='REQ_'+req_id,body=msg)

def transfer(user_id,nilai,req_id):
    channel = connection.channel()
    channel.exchange_declare(exchange='EX_TRANSFER',exchange_type='direct',durable=True)
    msg = {}
    msg['action'] = 'transfer'
    msg['user_id'] = user_id
    msg['sender_id'] = '1406559055'
    msg['nilai'] = nilai
    msg['type'] = 'request'
    msg['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
    msg = json.dumps(msg)
    channel.basic_publish(exchange='EX_TRANSFER',routing_key='REQ_'+req_id,body=msg)

if (arg[0] == 'register'):
    user_id = arg[1]
    nama = arg[2]
    req_id = arg[3]
    register(user_id,nama,req_id)
elif (arg[0] == 'saldo'):
    user_id=arg[1]
    req_id=arg[2]
    saldo(user_id,req_id)
elif (arg[0] == 'transfer'):
    user_id=arg[1]
    nilai=arg[2]
    req_id=arg[3]
    transfer(user_id,nilai,req_id)
