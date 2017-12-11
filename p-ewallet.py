# ewallet.py
import pika, json, datetime, time
from peewee import *
from models import *
from playhouse.sqlite_ext import SqliteExtDatabase

database = SqliteExtDatabase('tugas2.db', journal_mode='WAL')
params = pika.URLParameters('amqp://sisdis:sisdis@172.17.0.3:5672')
connection = pika.BlockingConnection(params)
channel = ''
n=input()
arg=n.split(" ")
transfer_id = ''
transfer_nilai = 0

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
    connection.close()

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
    result = channel.queue_declare()
    queue_name = result.method.queue
    channel.queue_bind(exchange='EX_REGISTER',queue=queue_name,routing_key='RESP_1406559055')
    channel.basic_consume(response_register, queue=queue_name, no_ack=True)
    channel.basic_publish(exchange='EX_REGISTER',routing_key='REQ_'+req_id,body=msg)
    print ("[x] Register request published")
    print ("waiting response")
    channel.start_consuming()

def response_saldo(ch, method, properties, body):
    try :
        msg = json.loads(body.decode("utf-8"))
    except Exception as e:
        print ("[E] Receive :",body)
        print ("[E] Error :",e)
    try :
    print ("nilai_saldo = ",msg['nilai_saldo'])
    except Exception as e:
        print ("[E] Error :",e)
    connection.close()

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
    result = channel.queue_declare()
    queue_name = result.method.queue
    channel.queue_bind(exchange='EX_GET_SALDO',queue=queue_name,routing_key='RESP_1406559055')
    channel.basic_consume(response_saldo, queue=queue_name, no_ack=True)
    channel.basic_publish(exchange='EX_GET_SALDO',routing_key='REQ_'+req_id,body=msg)
    print ("[X] Saldo request published")
    print ("waiting response")
    channel.start_consuming()

def simpan(user_id,nilai):
    database.connect()
    try:
        user = User.get(npm=user_id)
        user.saldo += nilai
        print ("[B] Saldo anda ",user.saldo)
        user.save()
    except Exception as e:
        print ("[E] Error :",e)
    database.close()

def ambil(user_id,nilai):
    database.connect()
    try:
        user = User.get(npm=user_id)
        user.saldo -= nilai
        print ("[B] Saldo anda ",user.saldo)
        user.save()
    except Exception as e:
        print ("[E] Error :",e)
    database.close()

def response_transfer(ch, method, properties, body):
    try :
        msg = json.loads(body.decode("utf-8"))
    except Exception as e:
        print ("[E] Receive :",body)
        print ("[E] Error :",e)
    try :
        print ("status_transfer = ",msg['status_transfer'])
    except Exception as e:
        print ("[E] Error :",e)
    if msg['status_transfer'] == 1 :
        ambil(transfer_id,transfer_nilai)
        print ("[X] Transfer berhasil! ")
    elif msg['status_transfer'] == -2 :
        print ("[E] Quorum tidak terpenuhi")
    elif msg['status_transfer'] == -1 :
        print ("[E] USER_ID tidak terdaftar, silahkan melakukan registrasi")
    elif msg['status_transfer'] == -4 :
        print ("[E] Terjadi kesalahan pada database")
    elif msg['status_transfer'] == -5 :
        print ("[E] Nilai transfer diluar ketentuan")
    elif msg['status_transfer'] == -99 :
        print ("[E] Terjadi kesalahan, belum terdefinisi")
    connection.close()

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
    result = channel.queue_declare()
    queue_name = result.method.queue
    channel.queue_bind(exchange='EX_TRANSFER',queue=queue_name,routing_key='RESP_1406559055')
    channel.basic_consume(response_transfer, queue=queue_name, no_ack=True)
    channel.basic_publish(exchange='EX_TRANSFER',routing_key='REQ_'+req_id,body=msg)
    print ("[X] Transfer request published")
    print ("waiting response")
    channel.start_consuming()

def response_totalSaldo(ch, method, properties, body):
    try :
        msg = json.loads(body.decode("utf-8"))
    except Exception as e:
        print ("[E] Receive :",body)
        print ("[E] Error :",e)
    try :
        print ("nilai_saldo = ",msg['nilai_saldo'])
    except Exception as e:
        print ("[E] Error :",e)
    connection.close()

def totalSaldo(user_id,req_id):
    channel = connection.channel()
    channel.exchange_declare(exchange='EX_GET_TOTAL_SALDO',exchange_type='direct',durable=True)
    msg = {}
    msg['action'] = 'get_total_saldo'
    msg['user_id'] = user_id
    msg['sender_id'] = '1406559055'
    msg['type'] = 'request'
    msg['ts']= '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
    msg = json.dumps(msg)
    result = channel.queue_declare()
    queue_name = result.method.queue
    channel.queue_bind(exchange='EX_GET_TOTAL_SALDO',queue=queue_name,routing_key='RESP_1406559055')
    channel.basic_consume(response_totalSaldo, queue=queue_name, no_ack=True)
    channel.basic_publish(exchange='EX_GET_TOTAL_SALDO',routing_key='REQ_'+req_id,body=msg)
    print ("[X] Get total saldo request published")
    print ("waiting response")
    channel.start_consuming()

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
    transfer_id = user_id
    transfer_nilai = nilai
    req_id=arg[3]
    transfer(user_id,nilai,req_id)
elif (arg[0] == 'simpan'):
    user_id=arg[1]
    nilai = arg[2]
    simpan(user_id,nilai)
elif (arg[0] == 'ambil'):
    user_id=arg[1]
    nilai = arg[2]
    ambil(user_id,nilai)
elif (arg[0] == 'totalSaldo'):
    user_id=arg[1]
    req_id=arg[2]
