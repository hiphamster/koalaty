import re
import sys
import csv

def parse_contacts(_csv=None):
    contacts = [] 
    if _csv:
        with open(_csv) as contacts_f:
            reader = csv.DictReader(contacts_f, quoting=csv.QUOTE_ALL)
            for line in reader:
                line['mobile_phone'] = re.sub(r'[^\d]', '', line['mobile_phone'])
                row = dict([
                    ('first_name', line['first_name']),
                    ('last_name', line['last_name']),
                    ('mobile_phone', line['mobile_phone']),
                    ('address1', line['address1']),
                    ('city', line['city']),
                    ('state', line['state']),
                    ('zip', line['zip']),
                    ('personal_facebook_url', line['personal_facebook_url']),
                ])
                contacts.append(row)

    return contacts

def write_csv(contacts=None):
    fields = ('first_name', 'last_name', 'mobile_phone', 
              'address1', 'city', 'state', 'zip', 'personal_facebook_url') 

    if (contacts):
       with open('new_contacts.csv', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fields)
            writer.writeheader()
            writer.writerows(contacts)


if __name__ == '__main__': 
    if (len(sys.argv) > 1):
        c = parse_contacts(_csv=sys.argv[1])
        # print(c)
        write_csv(contacts=c)
    else:
        print("Nothing to do")
