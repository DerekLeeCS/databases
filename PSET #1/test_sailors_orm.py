from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import text
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
    ans = [(101, 'Interlake')]

    inner2Statement = select(Reservation.bid, func.count().label("counts")) \
        .select_from(Reservation) \
        .group_by(Reservation.bid) \
        .subquery()
    innerStatement = select(Reservation.bid, func.max(text("counts"))) \
        .select_from(inner2Statement) \
        .subquery()
    statement = select(Boat.bid, Boat.bname) \
        .select_from(Boat) \
        .join(innerStatement)
    results = session.execute(statement).fetchall()
    print(str(statement))
    assert results == ans
