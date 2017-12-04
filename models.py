from peewee import *
import datetime

sqlite_db = SqliteDatabase('/root/tugas2/tugas2.db')

class BaseModel(Model):
	class Meta:
		database = sqlite_db

class User(BaseModel):
	name = CharField(default='')
	npm = CharField(unique=True)
	saldo = DoubleField(default=0)

class Quorum(BaseModel):
	npm = CharField(unique=True)
	timestamp = DateTimeField(default=datetime.datetime.now)


def create_tables():
	sqlite_db.connect()
	sqlite_db.create_tables([User,Quorum])

if __name__ == "__main__":
	create_tables()

