from sqlalchemy import create_engine, Column, Integer, String, DateTime, PrimaryKeyConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, backref, relationship
import datetime

engine = create_engine('sqlite:///sailors.db', echo = True)

Session = sessionmaker(bind = engine)
session = Session()

def initTable(tableClass, rawData):
    # Reset the table
    tableClass.__table__.drop(engine, checkfirst=True)
    tableClass.__table__.create(engine, checkfirst=True)

    # Insert data
    data = [tableClass(x) for x in rawData]
    session.bulk_save_objects(data)
    session.commit()

sailors = [
	(22,"dusting",7,45.0),
	(23,"emilio",7,45.0),
	(24,"scruntus",1,33.0),
	(29,"brutus",1,33.0),
	(31,"lubber",8,55.5),
	(32,"andy",8,25.5),
	(35,"figaro",8,55.5),
	(58,"rusty",10,35),
	(59,"stum",8,25.5),
	(60,"jit",10,35),
	(61,"ossola",7,16),
	(62,"shaun",10,35),
	(64,"horatio",7,16),
	(71,"zorba",10,35),
	(74,"horatio",9,25.5),
	(85,"art",3,25.5),
	(88,"kevin",3,25.5),
	(89,"will",3,25.5),
	(90,"josh",3,25.5),
	(95,"bob",3,63.5),
]

boats = [
	(101,"Interlake","blue",45),
	(102,"Interlake","red",45),
	(103,"Clipper","green",40),
	(104,"Clipper","red",40),
	(105,"Marine","red",35),
	(106,"Marine","green",35),
	(107,"Marine","blue",35),
	(108,"Driftwood","red",35),
	(109,"Driftwood","blue",35),
	(110,"Klapser","red",30),
	(111,"Sooney","green",28),
	(112,"Sooney","red",28),
]

reserves = [
	(22,101,"1998-10-10"),
	(22,102,"1998-10-10"),
	(22,103,"1998-08-10"),
	(22,104,"1998-07-10"),
	(23,104,"1998-10-10"),
	(23,105,"1998-11-10"),
	(24,104,"1998-10-10"),
	(31,102,"1998-11-10"),
	(31,103,"1998-11-06"),
	(31,104,"1998-11-12"),
	(35,104,"1998-08-10"),
	(35,105,"1998-11-06"),
	(59,105,"1998-07-10"),
	(59,106,"1998-11-12"),
	(59,109,"1998-11-10"),
	(60,106,"1998-09-05"),
	(60,106,"1998-09-08"),
	(60,109,"1998-07-10"),
	(61,112,"1998-09-08"),
	(62,110,"1998-11-06"),
	(64,101,"1998-09-05"),
	(64,102,"1998-09-08"),
	(74,103,"1998-09-08"),
	(88,107,"1998-09-08"),
	(88,110,"1998-09-05"),
	(88,110,"1998-11-12"),
	(88,111,"1998-09-08"),
	(89,108,"1998-10-10"),
	(89,109,"1998-08-10"),
	(90,109,"1998-10-10"),
]

Base = declarative_base()

class Sailor(Base):
    __tablename__ = 'sailors'

    sid = Column(Integer, primary_key=True)
    sname = Column(String)
    rating = Column(Integer)
    age = Column(Integer)

    def __init__(self, data):
        self.sid = data[0]
        self.sname = data[1]
        self.rating = data[2]
        self.age = data[3]

    def __repr__(self):
        return "<Sailor(id=%s, name='%s', rating=%s, age=%s)>" % (self.sid, self.sname, self.rating, self.age)

class Boat(Base):
    __tablename__ = 'boats'

    bid = Column(Integer, primary_key=True)
    bname = Column(String)
    color = Column(String)
    length = Column(Integer)

    reservations = relationship('Reservation',
                                backref=backref('boat', cascade='delete'))

    def __init__(self, data):
        self.bid = data[0]
        self.bname = data[1]
        self.color = data[2]
        self.length = data[3]
    
    def __repr__(self):
        return "<Boat(id=%s, name='%s', color=%s)>" % (self.bid, self.bname, self.color)

class Reservation(Base):
    __tablename__ = 'reserves'
    __table_args__ = (PrimaryKeyConstraint('sid', 'bid', 'day'), {})

    sid = Column(Integer, ForeignKey('sailors.sid'))
    bid = Column(Integer, ForeignKey('boats.bid'))
    day = Column(DateTime)

    sailor = relationship('Sailor')

    def __init__(self, data):
        self.sid = data[0]
        self.bid = data[1]
        self.day = datetime.datetime.strptime(data[2], "%Y-%m-%d")

    def __repr__(self):
        return "<Reservation(sid=%s, bid=%s, day=%s)>" % (self.sid, self.bid, self.day)

# Drop, Create, Insert Tables
initTable(Sailor, sailors)
initTable(Boat, boats)
initTable(Reservation, reserves)
