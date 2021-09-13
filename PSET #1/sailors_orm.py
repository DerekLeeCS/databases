from sqlalchemy import create_engine, Column, Integer, String, DateTime, PrimaryKeyConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, backref, relationship
from data import sailors, boats, reserves
import datetime

engine = create_engine('sqlite:///sailors.db', echo=True)

Session = sessionmaker(bind=engine)
session = Session()

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

def initTable(tableClass, rawData):
    # Reset the table
    tableClass.__table__.drop(engine, checkfirst=True)
    tableClass.__table__.create(engine, checkfirst=True)

    # Insert data
    data = [tableClass(x) for x in rawData]
    session.bulk_save_objects(data)
    session.commit()

# Drop, Create, Insert Tables
initTable(Sailor, sailors)
initTable(Boat, boats)
initTable(Reservation, reserves)
