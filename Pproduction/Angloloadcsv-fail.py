from faker import Faker
import csv
output=open('/home/demilsonfayika/guidpipeline/valid.csv','w')
fake=Faker()
header=['guid','age','birthday','signup_date','account_type']
mywriter=csv.writer(output)
mywriter.writerow(header)
 for r in range(1000):
    mywriter.writerow([fake.guid(),fake.random_int(min=1, max=100, step=1), fake.street_address(), fake.city(),fake.state(),fake.zipcode(),fake.longitude(),fake.latitude()])
output.close()
print('{"status":"complete"}')
