# import sys
import os
import csv
import typer

app = typer.Typer()

@app.command()
def parse_orders(in_file: str = typer.Argument(help=f'file to process'), 
               out_file: str = typer.Option('', help='file to use for output')):

    if not out_file:
        out_file = f'{in_file}_out.csv'

    if os.path.exists(in_file):
        with open(in_file, 'r') as data:
            csv_reader = csv.DictReader(data, quoting=csv.QUOTE_ALL)
            with open(out_file, 'w') as out:
                for line in csv_reader:
                    _date = line['create_date'].split(' ')[0]
                    # products
                    # Salmon CS x 109 @ $1.5 each,Trout HS x 50 @ $1 each,Mackerel CS x 14 @ $1 each
                    print(f"{line['primary_contact_name']}, {_date}, {line['products']}, {line['amount']}", file=out)


if __name__ == '__main__':
    app()
