#query_db.py
from peewee import *
from models import *

database = SqliteDatabase('tugas2.db')
database.connect()
try :
	query = Quorum.select()
	i = 0
	for user in query:
		i = i + 1
		print ("npm: ",user.npm," ; timestamp: ",user.timestamp)
	print ("total: ",i)
except Exception as e:
	print (e)
database.close()
