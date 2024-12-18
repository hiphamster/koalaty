import csv
import typer
import re

# import mysql
import sqlalchemy

from sqlalchemy import MetaData, Table, create_engine, insert, update

from config import settings

#class ParseException(Exception):
#    # o_str: original string
#    def __init__(self, message, o_str=None):
#        super().__init__(message)
#        self.o_str = o_str

engine = create_engine(settings.DB_URL)

app = typer.Typer()


@app.command()
def parse(export_file: str = typer.Argument(help='Contacts export from vipecloud')):
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


@app.command()
def load(in_file: str = typer.Argument(help='csv file to process')):
    """Load parsed data into the db"""

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

if __name__ == '__main__':
    app()

