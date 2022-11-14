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
    # print("test")
    # May not be necessary
    fields = ['sender_pk', 'receiver_pk', 'buy_currency', 'sell_currency', 'buy_amount', 'sell_amount']
    order_obj = Order(**{f: order[f] for f in fields})

    session.add(order_obj)
    session.commit()
    # print(order_obj.id)
    #print(order_obj.sell_amount)

    match = find_match(order)
    if match != None:
        '''
        tstamp = datetime
        order['filled'] = tstamp
        match.filled = tstamp
        order['counterparty_id'] = match.id
        #match.counterpart_id = orderid
        '''


def find_match(order):
    sell_currency = order['sell_currency']
    buy_currency = order['buy_currency']
    potential_matches = session.query(Order).filter(Order.buy_currency == sell_currency,
                                                    Order.sell_currency == buy_currency).all()
    #potential_matches = session.query(Order).all()
    # print("test")
    for o in potential_matches:
        print(o.id)
        if o.filled is not None:
            print(o.filled)
        if o.sell_amount / o.buy_amount >= order['buy_amount'] / order['sell_amount']:
            return o
    return None
