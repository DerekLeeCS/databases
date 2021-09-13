from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from sailors_orm import Sailor, Boat, Reservation

engine = create_engine('sqlite:///sailors.db', echo=True)

Session = sessionmaker(bind=engine)
session = Session()

def test_q1():
    ans = []

    statement = select(Sailor.sid)
    results = session.execute(statement).fetchall()
    assert results == ans