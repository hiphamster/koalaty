import csv
from sqlalchemy.engine import connection_memoize
import typer
import re
import os

import datetime

# import mysql
import sqlalchemy

from sqlalchemy import MetaData, Table, create_engine, engine_from_config, insert_sentinel, select, update, delete, literal, and_

from sqlalchemy.dialects.mysql import insert

from config import settings

class ParseProductsException(Exception):
    # o_str: original string
    def __init__(self, message, o_str=None):
        super().__init__(f'ParseProductsException: {message}')
        self.o_str = o_str

#class ParseException(Exception):
#    # o_str: original string
#    def __init__(self, message, o_str=None):
#        super().__init__(message)
#        self.o_str = o_str

engine = create_engine(settings.DB_URL)

metadata = MetaData()

Customers = Table('customers', metadata, autoload_with=engine)
Orders = Table('orders', metadata, autoload_with=engine)
OrderLines = Table('order_lines', metadata, autoload_with=engine)
Products = Table('products', metadata, autoload_with=engine)




app = typer.Typer()


@app.command()
def parse_contacts(export_file: str = typer.Argument(help='Contacts export from vipecloud')):
    """Parse export from vipecloud, and create a csv"""

    # used to rename fields, 
    #XXX currently only changes 'address1' to 'street'
    contacts_fields = {'first_name':'first_name','last_name':'last_name',
                       'mobile_phone':'mobile_phone', 'address1':'street', 
                       'city':'city', 'state':'state', 'zip':'zip', 
                       'personal_facebook_url':'personal_facebook_url'}

    parsed_contacts_csv = 'parsed_contacts.csv'
    with open(export_file, 'r') as vipe_export:
        with open(parsed_contacts_csv, 'w') as parsed_csv:
            reader = csv.DictReader(vipe_export, quoting=csv.QUOTE_ALL)
            writer = csv.DictWriter(parsed_csv, fieldnames=list(contacts_fields.values()))
            writer.writeheader()
            for line in reader:
                #remove all non-numeric
                line['mobile_phone'] = re.sub(r'[^\d]', '', line['mobile_phone'])
                row = {k:line[k] for k in list(contacts_fields.keys())}
                # rename column
                row['street'] = row.pop('address1')
                writer.writerow(row)


#TODO specigy in_file default - parsed_contacts.csv
@app.command()
def load_contacts(in_file: str = typer.Argument(help='csv file to process')):
    """Load parsed contacts into the db"""

    values = []

    # read csv
    with open(in_file) as _csv:
        reader = csv.DictReader(_csv, quoting=csv.QUOTE_ALL)
        values = list(reader)

    # Load data into MySQL
    try:
        with engine.connect() as conn:
            metadata = MetaData()
            Customers = Table('customers', metadata, autoload_with=engine)

            # result = conn.execute(insert(Customers), values)
            for row in values:
                try:
                    r = conn.execute(insert(Customers), row)
                    # print(f"lastrowid: {r.lastrowid}")
                    print(f'Row inserted: {r.lastrowid} - {row["first_name"]} {row["last_name"]}')

                except sqlalchemy.exc.IntegrityError as e:
                    # record exists
                    if (e.orig.errno == 1062 and e.orig.sqlstate == '23000'):
                        print(f'Record exists - {row["first_name"]} {row["last_name"]}')
                        # print(e.orig.errno) # 1062
                        # print(e.orig.sqlstate) # 23000
                        # print(e.orig.msg) 
                        pass

                except sqlalchemy.exc.DBAPIError as e:
                    print(f'Database operation failed')
                    with open(f'{in_file}.err', 'a') as _err:
                        print(e, '\n', file=_err)

            conn.commit()
            # print(f"{result.rowcount} records processed")

    except sqlalchemy.exc.DBAPIError as e:
        print(f'Database operation failed: {e}')


@app.command()
def parse_orders(export_file: str = typer.Argument(help=f'file to process')):
    """Parse export from vipecloud, and create a csv"""

    # used to rename fields
    orders_fields = {'primary_contact_name':'primary_contact_name',
                     'create_date':'create_date', 
                     'stage_name':'stage_name',
                     'products':'products', 
                     'amount':'amount'}

    #FYI - sqlalchemy has 'case' statement, which 
    # takes a mapping
    # stage_map = {'Order placed':'placed',
    #            'Ready for pickup': 'ready',
    #            'Closed - Picked-up, payment received':'closed_won',
    #            'Closed - Lost':'closed_lost',
    #            'Picked-up, Awaiting payment':'awaiting_payment'}

    parsed_orders_csv = 'parsed_orders.csv'
    with open(export_file, 'r') as vipe_export:
        with open(parsed_orders_csv, 'w') as parsed_csv:
            reader = csv.DictReader(vipe_export, quoting=csv.QUOTE_ALL)
            writer = csv.DictWriter(parsed_csv, fieldnames=list(orders_fields.values()))
            writer.writeheader()
            for line in reader:
                row = {k:line[k] for k in list(orders_fields.keys())}
                writer.writerow(row)


def parse_products(products: str):
    """Convert csv product lines into a dict"""

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

        return out 

    except Exception:
        raise ParseProductsException(f"Culdn't parse products", o_str=products)

#TODO - add unique constraint to orders and order_lines
#Notes -
# on_duplicate_key_update() can't be used because if Order hasn't changed, order lines
# can't be updated (on_duplicate_key_update won't catch it). What needs to be done -
# try to do an insert, on 1062 (23000): Duplicate entry do an update 
@app.command()
def load_orders_old(in_file: str = typer.Argument(help=f'file to process')):
    """Load parsed orders into the db"""

    try:
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
                # primary_contact_name,create_date,stage_name,products,amount
                for line in csv_reader:
                    try:
                        _date = datetime.datetime.strptime(
                            line['create_date'], '%Y-%m-%d %H:%M:%S')

                        products = parse_products(line['products'])

                        # must clean up all extra spaces
                        (f_name,l_name) = re.split(r'\s+', line['primary_contact_name'].strip())

                        select_customer = select(Customers.c.id).where(
                            and_(Customers.c.first_name == f_name, 
                                 Customers.c.last_name == l_name))

                        #FYI result is a list of named tuples
                        customer = conn.execute(select_customer).one()

                        #  Insert.on_duplicate_key_update()

                        insert_orders = insert(Orders).values(
                            customer_id=customer.id,
                            order_date=_date,
                            stage_name=line['stage_name']
                        )

                        # print(f'{insert_orders}\ncustomer_id: {customer.id}, {_date}, {line["stage_name"]}')

                        upsert_orders = insert_orders.on_duplicate_key_update(
                            stage_name=insert_orders.inserted.stage_name)

                        # print(f'{upsert_orders}\ncustomer_id: {customer.id}, {_date}, {line["stage_name"]}')

                        #XXX using from_select XXX
                        # customers table
#                        select_customers = select(
#                            Customers.c.id, literal(_date).label('order_date')).where(
#                            and_(Customers.c.first_name == f_name, 
#                                 Customers.c.last_name == l_name))
#
#                        # ORDERS table
#                        insert_orders = insert(Orders).from_select(
#                            ['customer_id', 'order_date'], select_customers)
                        #XXX ------------------ XXX

                        # r_orders = conn.execute(insert_orders)
                        r_orders = conn.execute(upsert_orders)
                        print(f'orders rows affected: {r_orders.rowcount} {r_orders.lastrowid}')

                        # rowcount == 2 means insert failed, and update succeeded (1 + 1)
#                        if r_orders.rowcount == 2:
#                            # delete order_lines for that order, and recreate later
#                            del_order_lines = delete(OrderLines).where(OrderLines.c.order_id == r_orders.lastrowid)
#                            d_orderlines = conn.execute(del_order_lines)
#                            print(f'Updating order {r_orders.lastrowid}, {d_orderlines.rowcount} orderlines deleted')
#                        
#                        # ORDER_LINES table
#                        for (_prod, _qty) in products.items():
#
#                            select_products = select(
#                                Products.c.id.label('product_id'),
#                                literal(r_orders.lastrowid).label('order_id'),
#                                literal(int(_qty)).label('quantity'),
#                                (int(_qty) * Products.c.unit_price).label('total'),
#                            ).where(
#                                Products.c.name == _prod
#                            )
#
#                            insert_order_lines = insert(OrderLines).from_select(
#                                ['product_id', 'order_id', 'quantity', 'total'], select_products
#                            )
#
#                            r_order_lines = conn.execute(insert_order_lines)
#                            print(f'order_lines rows affected: {r_order_lines.rowcount} {r_order_lines.lastrowid}')

                        conn.commit()

                    except ValueError as e:
                        with open(f'{in_file}.err', 'a') as err:
                            print(f'{e}\n{line}\n----', file=err)
                        continue
                    except ParseProductsException as e:
                        with open(f'{in_file}.err', 'a') as err:
                            print(f'ParseProductsException: {e}\n{line}\nproducts str:\n{e.o_str}\n----', file=err)
                        continue
                    except sqlalchemy.exc.DBAPIError as e:
                        # 1062 (23000): Duplicate entry '26-2024-12-06 22:42:53' for key 'unique_customer_date'
                        with open(f'{in_file}.err', 'a') as err:
                            print(f'Database operation failed {e}', file=err)
                        break
                        

    except sqlalchemy.exc.DBAPIError as e:
        print(f'Database operation failed: {e}')



def insert_order_lines(order_id: int, products: dict, conn):

    # ORDER_LINES table
    for (_prod, _qty) in products.items():

        select_products = select(
            Products.c.id.label('product_id'),
            literal(order_id).label('order_id'),
            literal(int(_qty)).label('quantity'),
            (int(_qty) * Products.c.unit_price).label('total'),
        ).where(
            Products.c.name == _prod
        )

        order_lines_insert = insert(OrderLines).from_select(
            ['product_id', 'order_id', 'quantity', 'total'], select_products
        )

        r_order_lines = conn.execute(order_lines_insert)
        print(f'insert order_lines rows affected: {r_order_lines.rowcount} {r_order_lines.lastrowid}')



def upsert(conn, customer_id: int, order_date: datetime.datetime, products: dict, order: dict):

    try:
        # insert_orders = insert(Orders).values()

        r = conn.execute(insert(Orders), {
            'customer_id':customer_id,
            'order_date': order_date,
            'stage_name': order['stage_name']
        })

        insert_order_lines(r.lastrowid, products, conn)

    except sqlalchemy.exc.DBAPIError as e:
        # 1062 (23000): Duplicate entry '26-2024-12-06 22:42:53' for key 'unique_customer_date'
        # e.orig.__class__ - mysql.connector.errors.IntegrityError
        # 'unique__cust_id__order_date'
        if (str(e.orig).find('unique__cust_id__order_date') != -1):
            print(f'order record exist, need to do an update')

            # update order
            select_order = select(Orders.c.id).where(and_(
                Orders.c.customer_id == customer_id,
                Orders.c.order_date == order_date
            ))
            # 
            _order = conn.execute(select_order).one()
            print(f'Order selected: {_order.id}')

            update_order = update(Orders).where(Orders.c.id == _order.id)
            update_result = conn.execute(update_order, {'stage_name': order['stage_name']})  
            print(f'Order update: {update_result.rowcount} rowcount, {update_result.lastrowid} lastrowid')

            # delete order_lines
            delete_order_lines = delete(OrderLines).where(OrderLines.c.order_id == _order.id)
            delete_result = conn.execute(delete_order_lines)
            print(f'OrderLines delete: {delete_result.rowcount} rowcount')

            # create order_lines
            insert_order_lines(_order.id, products, conn)

        else:
            with open(f'{"load_orders"}.err', 'a') as err:
                print(f'Database operation failed {e}', file=err)
                # print(f'orig: {e.orig}')


@app.command()
def load_orders(in_file: str = typer.Argument(help=f'file to process')):
    """Load parsed orders into the db"""

    with engine.connect() as conn:

        with open(in_file, 'r') as data:
            # csv.DictReader assumes first line contains col headers
            csv_reader = csv.DictReader(data, quoting=csv.QUOTE_ALL)
            # primary_contact_name,create_date,stage_name,products,amount
            for order in csv_reader:
                try:
                    _date = datetime.datetime.strptime(order['create_date'], '%Y-%m-%d %H:%M:%S')

                    products = parse_products(order['products'])

                    # remove spaces around names
                    (f_name,l_name) = re.split(r'\s+', order['primary_contact_name'].strip())

                    select_customer = select(Customers.c.id).where(
                        and_(Customers.c.first_name == f_name, 
                             Customers.c.last_name == l_name))

                    #FYI result is a list of named tuples
                    customer = conn.execute(select_customer).one()

                    upsert(conn, customer.id, _date, products, order)

                except ValueError as e:
                    with open(f'{in_file}.err', 'a') as err:
                        print(f'{e}\n{order}\n----', file=err)

                except ParseProductsException as e:
                    with open(f'{in_file}.err', 'a') as err:
                        print(f'{e}\n{order}\nproducts str:\n{e.o_str}\n----', file=err)

            conn.commit()

if __name__ == '__main__':
    app()

