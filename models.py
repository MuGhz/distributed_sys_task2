from peewee import *

sqlite_db = SqliteDatabase('/root/tugas2/tugas2.db')

class BaseModel(Model):
	class Meta:
		database = sqlite_db

class User(BaseModel):
	name = CharField(default='')
	npm = CharField(unique=True)
	saldo = DoubleField(default=0)
	ts = CharField(default='')

def create_tables():
	sqlite_db.connect()
	sqlite_db.create_tables([User])

if __name__ == "__main__":
	create_tables()

