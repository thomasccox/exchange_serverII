from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()


def process_order(order):
    # Your code here

    # May not be necessary
    fields = ['sender_pk', 'receiver_pk', 'buy_currency', 'sell_currency', 'buy_amount', 'sell_amount']
    order_obj = Order(**{f: order[f] for f in fields})

    session.add(order_obj)
    session.commit()

    match = find_match(order)
    if match != None:
        tstamp = datetime.timezone.utc
        order.filled = tstamp
        match.filled = tstamp
        order.counterparty_id = match.id
        match.counterpart_id = order.id



def find_match(order):
    sell_currency = order['sell_currency']
    buy_currency = order['buy_currency']
    potential_matches = session.query(Order).filter(Order.filled is None, Order.buy_currency == sell_currency,
                                                    Order.sell_currency == buy_currency)
    for o in potential_matches:
        if o.sell_amount / o.buy_amount >= order['buy_amount'] / order['sell_amount']:
            print("test")
            return o
    return None
