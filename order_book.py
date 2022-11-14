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
    #print(order_obj.id)
    #print(order_obj.sell_amount)

    match = find_match(order)
    #print(match.id)
    if match is not None:
        #print('\n')
        #print("Match")
        _order = session.get(Order, order_obj.id)
        tstamp = datetime.time()
        _order.filled = tstamp
        match.filled = tstamp
        _order.counterpart_id = match.id
        match.counterpart_id = _order.id



    else:
        #print('\n')
        #print("No Match")
        '''

        '''


def find_match(order):
    sell_currency = order['sell_currency']
    buy_currency = order['buy_currency']
    potential_matches = session.query(Order).filter(Order.buy_currency == sell_currency,
                                                    Order.sell_currency == buy_currency).all()
    #potential_matches = session.query(Order).all()
    # print("test")
    for o in potential_matches:
        #print(o.id)
        if o.filled is None:
            #print(o.filled)
            if o.sell_amount / o.buy_amount >= order['buy_amount'] / order['sell_amount']:
                return o
    return None
