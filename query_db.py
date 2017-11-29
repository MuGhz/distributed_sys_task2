#query_db.py
from peewee import *
from models import *

database = SqliteDatabase('tugas2.db')
database.connect()
try :
	query = User.select()
	i = 0
	for user in query:
		i = i + 1
		print ("name: ",user.name," ; npm: ",user.npm," ; saldo: ",user.saldo," ; timestamp: ",user.ts)
	print ("total: ",i)
except Exception as e:
	print (e)
database.close()
