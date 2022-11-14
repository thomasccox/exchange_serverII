from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()


def process_order(order, match=None):
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
        #_match = session.get(Order, match.id)
        tstamp = datetime.now()
        _order.filled = tstamp
        match.filled = tstamp
        _order.counterpart_id = match.id
        match.counterpart_id = _order.id

        if match.buy_amount > _order.sell_amount:
            child_buy = match.buy_amount - _order.sell_amount
            child_sell = (match.sell_amount/match.buy_amount)*(match.buy-_order.sell)
            profit = match.sell - child_sell
            child = match
        elif match.buy_amount < _order.sell_amount:
            child_sell = _order.sell_amount - match.buy_amount
            child_buy = (_order.sell_amount - match.buy_amount)*(_order.buy_amount/_order.sell_amount)
            child = _order
        if match.buy_amount != _order.sell_amount:
            child_order = {}
            child_order['sender_pk'] = child.sender_pk
            child_order['receiver_pk'] = child.receiver_pk
            child_order['buy_currency'] = child.buy_currency
            child_order['sell_currency'] = child.sell_currency
            child_order['buy_amount'] = child_buy
            child_order['sell_amount'] = child_sell
            fields = ['sender_pk', 'receiver_pk', 'buy_currency', 'sell_currency', 'buy_amount', 'sell_amount']
            child_obj = Order(**{f: child_order[f] for f in fields})

            session.add(child_obj)
            session.commit()


        session.commit()
        #test_order = session.get(Order, order_obj.id)
        #print(test_order.counterpart_id)




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
                #break
    return None
