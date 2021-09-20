from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import except_, text
from sailors_orm import Sailor, Boat, Reservation

engine = create_engine('sqlite:///sailors.db', echo=True)

Session = sessionmaker(bind=engine)
session = Session()

def test_q1():
    ans = [
        (101, 'Interlake', 2),
        (102, 'Interlake', 3),
        (103, 'Clipper', 3),
        (104, 'Clipper', 5),
        (105, 'Marine', 3),
        (106, 'Marine', 3),
        (107, 'Marine', 1),
        (108, 'Driftwood', 1),
        (109, 'Driftwood', 4),
        (110, 'Klapser', 3),
        (111, 'Sooney', 1),
        (112, 'Sooney', 1)
    ]

    innerStatement = select(Reservation.bid, func.count().label("num_reserves")) \
        .select_from(Reservation) \
        .group_by(Reservation.bid) \
        .having(text("num_reserves > 0")) \
        .subquery()
    statement = select(Boat.bid, Boat.bname, text("num_reserves")) \
        .select_from(Boat) \
        .join(innerStatement)
    results = session.execute(statement).fetchall()

    assert results == ans

def test_q4():
    ans = [(104, 'Clipper')]

    inner2Statement = select(Reservation.bid, func.count().label("counts")) \
        .select_from(Reservation) \
        .group_by(Reservation.bid) \
        .alias("temp1")
    innerWhereStatement = select(func.count().label("counts")) \
        .select_from(Reservation) \
        .group_by(Reservation.bid) \
        .alias("temp2")
    innerStatement = select(text("bid as bid_temp")) \
        .select_from(inner2Statement) \
        .where(text("counts = (" + str(select(func.max(text("counts"))) \
            .select_from(innerWhereStatement)) + ")")) \
        .alias("temp3")
    statement = select(Boat.bid, Boat.bname) \
        .select_from(Boat) \
        .join(innerStatement, text("bid = bid_temp"))   # This leads to a cartesian join??
    results = session.execute(statement).fetchall()
    assert results == ans

def test_q5():
    ans = [
        (29, 'brutus'),
        (32, 'andy'),
        (58, 'rusty'),
        (60, 'jit'),
        (71, 'zorba'),
        (74, 'horatio'),
        (85, 'art'),
        (90, 'vin'),
        (95, 'bob')
    ]

    mainStatement = select(Sailor.sid, Sailor.sname) \
        .select_from(Sailor)
    excludeInner2Statement = select(Boat.bid) \
        .select_from(Boat) \
        .where(Boat.color == "red") \
        .subquery()
    excludeInnerStatement = select(Reservation.sid) \
        .select_from(Reservation) \
        .join(excludeInner2Statement) \
        .subquery()
    excludeStatement = select(Sailor.sid, Sailor.sname) \
        .select_from(Sailor) \
        .join(excludeInnerStatement)
    statement = except_(mainStatement, excludeStatement)
    results = session.execute(statement).fetchall()
    assert results == ans

def test_q6():
    ans = [(35,)]

    statement = select(func.avg(Sailor.age)) \
        .select_from(Sailor) \
        .where(Sailor.rating == 10)
    results = session.execute(statement).fetchall()
    assert results == ans

def test_q7():
    ans = [
        (1, 24, 'scruntus', 33),
        (1, 29, 'brutus', 33),
        (3, 85, 'art', 25),
        (3, 89, 'dye', 25),
        (7, 61, 'ossola', 16),
        (7, 64, 'horatio', 16),
        (8, 32, 'andy', 25),
        (8, 59, 'stum', 25),
        (9, 74, 'horatio', 25),
        (9, 88, 'dan', 25),
        (10, 58, 'rusty', 35),
        (10, 60, 'jit', 35),
        (10, 62, 'shaun', 35),
        (10, 71, 'zorba', 35)
    ]

    innerStatement = select(Sailor.rating, func.min(Sailor.age).label("min_age")) \
        .select_from(Sailor) \
        .group_by(Sailor.rating) \
        .alias("temp1")
    statement = select(Sailor.rating, Sailor.sid, Sailor.sname, Sailor.age) \
        .select_from(Sailor) \
        .join(innerStatement, text("sailors.rating = temp1.rating AND age = min_age")) \
        .order_by(Sailor.rating, Sailor.sid)
    results = session.execute(statement).fetchall()
    assert results == ans
