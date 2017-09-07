from os import environ
import pandas
from sqlalchemy import create_engine
from pandas import DataFrame
import pandas as pd
from datetime import datetime

LB = environ['LOWER_PRICE_BOUND'] if 'LOWER_PRICE_BOUND' in environ else 40
UB = environ['UPPER_PRICE_BOUND'] if 'UPPER_PRICE_BOUND' in environ else 60
INC = environ['INCREMENTS'] if 'INCREMENTS' in environ else 2
ELASTICITY_VERSION = environ['ELASCTICITY_VERSION'] if 'ELASTICITY_VERSION' in environ else '0.0.1'
DOCKER_ML_OUTPUT_SCHEMA = environ['DOCKER_ML_OUTPUT_SCHEMA'] if 'DOCKER_ML_OUTPUT_SCHEMA' in environ else 'zzz_dbt_test_zubin'

def compute_slope_matrix(destination_engine, now=datetime.utcnow()):
    with destination_engine.connect() as connection:
        visitors = pd.read_sql(
            sql='SELECT date_trunc(\'month\', sent_at) as date, COUNT(DISTINCT anonymous_id) as visitors FROM web_prod.pages WHERE sent_at < \'{sent_at_filter}\' AND sent_at > \'2016-12-31\' GROUP BY date_trunc(\'month\', sent_at)'.format(
                sent_at_filter=now.date().isoformat()
            ),
            con=connection
        )
        conv = pd.read_sql(
            sql='SELECT date_trunc(\'month\', booked_at) as date, COUNT(DISTINCT order_id) as bookings FROM _dbt_output.orders WHERE booked_at < \'{booked_at_filter}\' AND booked_at > \'2016-12-31\' AND order_status IN {order_status_filter} GROUP BY date_trunc(\'month\', booked_at)'.format(
                booked_at_filter=now.date().isoformat(),
                order_status_filter="('booked', 'complete')"
            ),
            con=connection
        )

    visitors['date'] = pd.to_datetime(visitors['date'].dt.tz_localize(None))
    visitors = visitors.set_index('date')
    conv['date'] = pd.to_datetime(conv['date'].dt.tz_localize(None))
    conv = conv.set_index('date')
    conv['visitors'] = visitors['visitors']
    conv['rate'] = conv.bookings/conv.visitors
    
    rate_at_price = {40: conv[:'2017-06-14'].tail(len(conv['2017-06-15':])).rate.mean(), 45: conv['2017-06-15':].rate.mean()}
    slope = ((rate_at_price[45] - rate_at_price[40]))/float(5)

    for i in range(LB, UB+1, INC):
        conv[i] = (conv['rate'] + slope * (i-LB)) / conv['rate']

    print ("Global elasticity computed in %s seconds with %s NaN values." % (datetime.utcnow() - now, len(conv.visitors.isnull())))
    return conv[conv.visitors.isnull() == False].drop(['rate', 'bookings', 'visitors'], axis=1).tail(1)



if __name__ == "__main__":
    print (datetime.utcnow())
    gospel_engine = create_engine(environ['GOSPEL_DB_URL'])
    results_df = compute_slope_matrix(gospel_engine)
    with gospel_engine.connect() as connection:
        results_df['model_version'] = ELASTICITY_VERSION
        results_df.to_sql(con=connection, name='daily_elasticity_spread', if_exists='append', schema=DOCKER_ML_OUTPUT_SCHEMA, index=False)