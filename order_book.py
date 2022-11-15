from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()


def process_order(order, match=None):
    # Your code here
    fields = ['sender_pk', 'receiver_pk', 'buy_currency', 'sell_currency', 'buy_amount', 'sell_amount']
    order_obj = Order(**{f: order[f] for f in fields})

    session.add(order_obj)
    session.commit()

    existing = find_match(order)

    if existing is not None:

        order_obj.counterpart_id = existing.id
        order_obj.counterparty.append(existing)
        existing.counterpart_id = order_obj.id
        existing.counterparty.append(order_obj)
        #session.commit()
        tstamp = datetime.now()
        order_obj.filled = tstamp
        existing.filled = tstamp
        #session.commit()

        if existing.buy_amount > order_obj.sell_amount:
            child_buy = existing.buy_amount - order_obj.sell_amount
            child_sell = (existing.sell_amount / existing.buy_amount) * (existing.buy_amount - order_obj.sell_amount)
            child = existing
        elif existing.buy_amount < order_obj.sell_amount:
            child_sell = order_obj.sell_amount - existing.buy_amount
            child_buy = (order_obj.sell_amount - existing.buy_amount) * (order_obj.buy_amount / order_obj.sell_amount)
            child = order_obj

        if existing.buy_amount != order_obj.sell_amount:
            child_order = {}
            child_order['creator_id'] = child.id
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


def find_match(order):
    sell_currency = order['sell_currency']
    buy_currency = order['buy_currency']
    potential_matches = session.query(Order).filter(Order.buy_currency == sell_currency,
                                                    Order.sell_currency == buy_currency).all()

    for o in potential_matches:
        if o.filled is None:
            if o.sell_amount / o.buy_amount >= order['buy_amount'] / order['sell_amount']:
                return o
    return None
