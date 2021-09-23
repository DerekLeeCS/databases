from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import text, distinct
from sailors_orm import Sailor, Boat, Reservation

# Used to get DB connection
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Get parent dir
from dbInfo import Info

engine = create_engine('mysql+mysqlconnector://' + Info.connect + '/pset1', echo=True)

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
        (112, 'Sooney', 1),
    ]

    innerStatement = select(Reservation.bid, func.count().label("num_reserves")) \
        .select_from(Reservation) \
        .group_by(Reservation.bid) \
        .having(text("num_reserves > 0")) \
        .alias("temp1")
    statement = select(Boat.bid, Boat.bname, text("num_reserves")) \
        .select_from(Boat) \
        .join(innerStatement)
    results = session.execute(statement).fetchall()

    assert results == ans

def test_q2():
    ans = []

    innerStatement1 = select(text("sailors.*"), Reservation.bid) \
        .select_from(Sailor) \
        .join(Reservation) \
        .alias("temp1")
    innerStatement2 = select(text("sid"), text("sname"), Boat.bid).distinct() \
        .select_from(Boat) \
        .join(innerStatement1, text("boats.bid = temp1.bid")) \
        .where(Boat.color == "red") \
        .alias("temp2")
    innerStatement3 = select(text("sid"), text("sname"), func.count().label("counts")) \
        .select_from(innerStatement2) \
        .group_by(text("sid"), text("sname")) \
        .alias("temp3")
    innerStatement4 = select(func.count(distinct(Boat.bid)).label("max_count")) \
        .select_from(Boat) \
        .where(Boat.color == "red") \
        .alias("temp4")
    statement = select(text("sid"), text("sname")) \
        .select_from(innerStatement3) \
        .join(innerStatement4, text("counts = max_count"))
    results = session.execute(statement).fetchall()

    assert results == ans

def test_q3():
    ans = [
        (23, 'emilio'),
        (24, 'scruntus'),
        (35, 'figaro'),
        (61, 'ossola'),
        (62, 'shaun'),
    ]

    excludeInnerStatement = select(Boat.bid) \
        .select_from(Boat) \
        .where(Boat.color != "red") \
        .alias("temp1")
    excludeStatement = select(Reservation.sid) \
        .select_from(Reservation) \
        .join(excludeInnerStatement)
    excludeStatement = excludeStatement.compile(compile_kwargs={"literal_binds": True})
    innerStatement = select(Reservation.sid).distinct() \
        .select_from(Reservation) \
        .where(text("sid NOT IN (" + str(excludeStatement) + ")")) \
        .alias("temp2")
    statement = select(Sailor.sid, Sailor.sname) \
        .select_from(Sailor) \
        .join(innerStatement) \
        .order_by(Sailor.sid)
    results = session.execute(statement).fetchall()
    
    assert results == ans

def test_q4():
    ans = [(104, 'Clipper')]

    innerStatement1 = select(Reservation.bid, func.count().label("counts")) \
        .select_from(Reservation) \
        .group_by(Reservation.bid) \
        .alias("temp1")
    innerWhereStatement2 = select(func.count().label("counts")) \
        .select_from(Reservation) \
        .group_by(Reservation.bid) \
        .alias("temp2")
    innerStatement3 = select(text("bid AS bid_temp")) \
        .select_from(innerStatement1) \
        .where(text("counts = (" + str(select(func.max(text("counts"))) \
            .select_from(innerWhereStatement2)) + ")")) \
        .alias("temp3")
    statement = select(Boat.bid, Boat.bname) \
        .select_from(Boat) \
        .join(innerStatement3, Boat.bid == text("bid_temp"))
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
        (95, 'bob'),
    ]

    excludeInnerStatement2 = select(Boat.bid) \
        .select_from(Boat) \
        .where(Boat.color == "red") \
        .alias("temp1")
    excludeInnerStatement = select(Reservation.sid) \
        .select_from(Reservation) \
        .join(excludeInnerStatement2) \
        .alias("temp2")
    excludeStatement = select(Sailor.sid, Sailor.sname) \
        .select_from(Sailor) \
        .join(excludeInnerStatement)
    excludeStatement = excludeStatement.compile(compile_kwargs={"literal_binds": True})
    statement = select(Sailor.sid, Sailor.sname) \
        .select_from(Sailor) \
        .where(text("(sid, sname) NOT IN (" + str(excludeStatement) + ")"))
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
        (10, 71, 'zorba', 35),
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

def test_q8():
    ans = [
        (101, 22, 'dusting'),
        (101, 64, 'horatio'),
        (102, 22, 'dusting'),
        (102, 31, 'lubber'),
        (102, 64, 'horatio'),
        (103, 22, 'dusting'),
        (103, 31, 'lubber'),
        (103, 74, 'horatio'),
        (104, 22, 'dusting'),
        (104, 23, 'emilio'),
        (104, 24, 'scruntus'),
        (104, 31, 'lubber'),
        (104, 35, 'figaro'),
        (105, 23, 'emilio'),
        (105, 35, 'figaro'),
        (105, 59, 'stum'),
        (106, 60, 'jit'),
        (107, 88, 'dan'),
        (108, 89, 'dye'),
        (109, 59, 'stum'),
        (109, 60, 'jit'),
        (109, 89, 'dye'),
        (109, 90, 'vin'),
        (110, 88, 'dan'),
        (111, 88, 'dan'),
        (112, 61, 'ossola'),
    ]

    innerStatement1 = select(Reservation.bid, Reservation.sid, func.count().label("counts")) \
        .select_from(Reservation) \
        .group_by(Reservation.bid, Reservation.sid) \
        .alias("temp1")
    innerStatement2 = select(text("bid AS bid_temp"), func.max(text("counts")).label("max_counts")) \
        .select_from(innerStatement1) \
        .group_by(text("bid_temp")) \
        .alias("temp2")
    innerStatement3 = select(Reservation.bid, Reservation.sid, func.count().label("counts"), text("max_counts")) \
        .select_from(Reservation) \
        .join(innerStatement2, Reservation.bid == text("bid_temp")) \
        .group_by(Reservation.bid, Reservation.sid) \
        .alias("temp3")
    statement = select(text("bid"), Sailor.sid, Sailor.sname) \
        .select_from(Sailor) \
        .join(innerStatement3) \
        .where(text("counts = max_counts")) \
        .order_by(text("bid"), Sailor.sid)
    results = session.execute(statement).fetchall()

    assert results == ans    
