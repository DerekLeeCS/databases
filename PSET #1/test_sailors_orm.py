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

# def test_q7():
#     ans = [
#         (1, 24, 'scruntus', 33),
#         (3, 85, 'art', 25),
#         (7, 22, 'dusting', 16), # currently returns 'ossola' b/c multiple answers
#         (8, 31, 'lubber', 25),
#         (9, 74, 'horatio', 25),
#         (10, 58, 'rusty', 35)
#     ]

#     statement = select(Sailor.rating, Sailor.sid, Sailor.sname, func.min(Sailor.age)) \
#         .select_from(Sailor) \
#         .group_by(Sailor.rating)
#     results = session.execute(statement).fetchall()
#     assert results == ans