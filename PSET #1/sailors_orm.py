from typing import List, Tuple

from sqlalchemy import create_engine, Column, Integer, String, DateTime, PrimaryKeyConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, backref, relationship
import datetime

from sqlalchemy.sql.sqltypes import Numeric

from data import sailors, boats, reserves, reviews

# Used to get DB connection
from db_info import Info

engine = create_engine(Info.connection_string, echo=True)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


class Sailor(Base):
    __tablename__ = 'sailors'

    sid = Column(Integer, primary_key=True)
    sname = Column(String(20))
    rating = Column(Integer)
    age = Column(Integer)

    def __init__(self, data: Tuple[Integer, String, Integer, String]):
        self.sid = data[0]
        self.sname = data[1]
        self.rating = data[2]
        self.age = data[3]

    def __repr__(self):
        return "<Sailor(id=%s, name='%s', rating=%s, age=%s)>" % (self.sid, self.sname, self.rating, self.age)


class Boat(Base):
    __tablename__ = 'boats'

    bid = Column(Integer, primary_key=True)
    bname = Column(String(20))
    color = Column(String(20))
    length = Column(Integer)

    reservations = relationship('Reservation',
                                backref=backref('boat', cascade='delete'))

    def __init__(self, data: Tuple[Integer, String, String, Integer]):
        self.bid = data[0]
        self.bname = data[1]
        self.color = data[2]
        self.length = data[3]
    
    def __repr__(self):
        return "<Boat(id=%s, name='%s', color=%s)>" % (self.bid, self.bname, self.color)


class Reservation(Base):
    __tablename__ = 'reserves'

    rsrvid = Column(Integer, primary_key=True)
    sid = Column(Integer, ForeignKey('sailors.sid'))
    bid = Column(Integer, ForeignKey('boats.bid'))
    day = Column(DateTime)

    def __init__(self, data: Tuple[Integer, Integer, Integer, String]):
        self.rsrvid = data[0]
        self.sid = data[1]
        self.bid = data[2]
        self.day = datetime.datetime.strptime(data[3], "%Y/%m/%d")

    def __repr__(self):
        return "<Reservation(rsrvid=%s, sid=%s, bid=%s, day=%s)>" % (self.rsrvid, self.sid, self.bid, self.day)


class Review(Base):
    __tablename__ = 'reviews'
    __table_args__ = (PrimaryKeyConstraint('rsrvid', 'contents', 'rating', 'day'), {})
    
    rsrvid = Column(Integer, ForeignKey('reserves.rsrvid'))
    contents = Column(String(160))
    rating = Column(Numeric(2,1))
    day = Column(DateTime)

    def __init__(self, data: Tuple[Integer, String, Numeric, String]):
        self.rsrvid = data[0]
        self.contents = data[1]
        self.rating = data[2]
        self.day = datetime.datetime.strptime(data[3], "%Y/%m/%d")

    def __repr__(self):
        return "<Review(rsrvid=%s, contents=%s, rating=%s, day=%s)>" % (self.rsrvid, self.contents, self.rating, self.day)


def initTable(tables: List[Tuple[String, List]]):
    # Reset the tables
    [table[0].__table__.drop(engine, checkfirst=True) for table in tables]
    [table[0].__table__.create(engine, checkfirst=True) for table in reversed(tables)]

    # Insert data
    # For each table, uses data to initalize specified table class and then inserts data
    [session.bulk_save_objects([table[0](x) for x in table[1]])
        for table in reversed(tables)]
    session.commit()


# Drop, Create, Insert Tables
if __name__ == '__main__':
    initTable([
        (Review, reviews),
        (Reservation, reserves),
        (Sailor, sailors),
        (Boat, boats),
    ])
    print("Succeeded")
