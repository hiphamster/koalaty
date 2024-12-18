# import sys

# notes
# SQLAlchemy fetching / working with rows (named tuples)
# https://docs.sqlalchemy.org/en/20/tutorial/dbapi_transactions.html#fetching-rows

import csv
import re
import datetime

import typer
from sqlalchemy import MetaData, Table, create_engine, insert, select, and_, literal

from config import settings

app = typer.Typer()


# Create a SQLAlchemy engine
engine = create_engine(settings.DB_URL)

class ParseProductsException(Exception):
    # o_str: original string
    def __init__(self, message, o_str=None):
        super().__init__(message)
        self.o_str = o_str

# read orders, build json / dict for products, create order, create order_lines 
def parse_products(products: str):

    try:
        # Salmon CS x 109 @ $1.5 each,Trout HS x 50 @ $1 each,Mackerel CS x 14 @ $1 each
        prod = products.split(',')    

        out = {}

        for p in prod:
            # parts: [product, qty, price]
            parts = list(
                map(lambda x: x.strip(), re.split(r'[x@]', p))
            )
            # product => qty
            out[parts[0]] = parts[1]

            # actually i don't need the price - it will come from order_lines
            # clean-up price - convert '$1.5 each' into '1.5'
            # parts[2] = re.sub(r'[^\d.]','', parts[2])

        # print(out)
        return out 

    except Exception:
        raise ParseProductsException(f"Culdn't parse products", o_str=products)

@app.command()
def parse_orders(in_file: str = typer.Argument(help=f'file to process'), 
               out_file: str = typer.Option('', help='file to use for output')):

# def parse(export_file: str = typer.Argument(help='Contacts export from vipecloud')):

    if not out_file:
        out_file = f'{in_file}_out.csv'

    try:

        # log errors
        with open('load.err', 'w') as err:

            with engine.connect() as conn:

                metadata = MetaData()
                Customers = Table('customers', metadata, autoload_with=engine)
                Orders = Table('orders', metadata, autoload_with=engine)
                OrderLines = Table('order_lines', metadata, autoload_with=engine)
                Products = Table('products', metadata, autoload_with=engine)


                with open(in_file, 'r') as data:
                    # csv.DictReader assumes first line contains col headers
                    csv_reader = csv.DictReader(data, quoting=csv.QUOTE_ALL)
                    # with open(out_file, 'w') as out:
                    for line in csv_reader:
                        try:
                            _date = datetime.datetime.strptime(
                                line['create_date'], '%Y-%m-%d %H:%M:%S')

                            products = parse_products(line['products'])

                            (f_name,l_name) = line['primary_contact_name'].split(' ')

                            # conn.execute(insert(Customers), values)

                            # customers table
                            select_customers = select(
                                Customers.c.id, literal(_date).label('order_date')).where(
                                and_(Customers.c.first_name == f_name, 
                                     Customers.c.last_name == l_name))

                            # ORDERS table
                            insert_orders = insert(Orders).from_select(
                                ['customer_id', 'order_date'], select_customers)

                            r_orders = conn.execute(insert_orders)
                            print(f'orders rows affected: {r_orders.rowcount} {r_orders.lastrowid}')
                            
                            # ORDER_LINES table
                            for (_prod, _qty) in products.items():

                                select_products = select(
                                    Products.c.id.label('product_id'),
                                    literal(r_orders.lastrowid).label('order_id'),
                                    literal(int(_qty)).label('quantity'),
                                    (int(_qty) * Products.c.unit_price).label('total'),
                                ).where(
                                    Products.c.name == _prod
                                )

                                insert_order_lines = insert(OrderLines).from_select(
                                    ['product_id', 'order_id', 'quantity', 'total'], select_products
                                )

                                r_order_lines = conn.execute(insert_order_lines)
                                print(f'order_lines rows affected: {r_order_lines.rowcount} {r_order_lines.lastrowid}')

                            conn.commit()

                        except ValueError as e:
                            print(f'ValueError: {e}\n{line}\n----', file=err)
                            continue
                        except ParseProductsException as e:
                            print(f'ParseProductsException: {e}\n{line}\nproducts str:\n{e.o_str}\n----', file=err)
                            continue
                        except Exception as e:
                            print(f'Exception: {e}\n----', file=err)
                            raise e

                        # print(f"{line['primary_contact_name']}, {_date}, {line['products']}, {line['amount']}", file=out)

    except FileNotFoundError as e:
        print(f'File not found: {e}')
    # except Exception as e:
    #   print(f'An error has occured: {e}')

if __name__ == '__main__':
    app()
