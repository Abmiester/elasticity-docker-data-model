from os import environ
import pandas
from sqlalchemy import create_engine
from pandas import DataFrame

LB = environ['LOWER_PRICE_BOUND'] if 'LOWER_PRICE_BOUND' in environ else 40
UB = environ['UPPER_PRICE_BOUND'] if 'UPPER_PRICE_BOUND' in environ else 60
INC = environ['INCREMENTS'] if 'INCREMENTS' in environ else 2

def daily_market_metrics(destination_engine):
    with destination_engine.connect() as connection:
        visitors = connection.execute("SELECT date_trunc('day', sent_at at time zone 'UTC') as date, COUNT(DISTINCT anonymous_id) as visitors FROM web_prod.pages GROUP BY date_trunc('day', sent_at at time zone 'UTC')")
        bookings = connection.execute("SELECT date_trunc('day', booked_at at time zone 'UTC') as date, COUNT(DISTINCT id) as bookings FROM kinetic.reservations_order WHERE order_status!='pending' GROUP BY date_trunc('day', booked_at at time zone 'UTC')")

    conv = DataFrame.from_records(visitors.fetchall(), columns=['date', 'visitors']).set_index("date")
    rf = DataFrame.from_records(bookings.fetchall(), columns=['date', 'bookings']).set_index("date")
    conv['bookings'] = rf['bookings']
    conv['rate'] = conv.bookings/conv.visitors
    rate_at_price = {40: conv[:'2017-06-14'].tail(len(conv['2017-06-15':])).rate.mean(), 45: conv['2017-06-15':].rate.mean()}
    slope = ((rate_at_price[45] - rate_at_price[40]))/float(5)
    result = conv['2017-06-16':]

    for i in range(LB, UB+1, INC):
        result['rate_at_' + str(i)] = result['rate'] + slope * (i-LB)

    with destination_engine.connect() as connection:
        result.drop('rate', axis=1).tail(1).to_sql(con=connection, name='daily_global_test', if_exists='append', schema='forecast')


if __name__ == "__main__":
    gospel_engine = create_engine(environ['GOSPEL_DB_URL'])
    daily_market_metrics(gospel_engine)