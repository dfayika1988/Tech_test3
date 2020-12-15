from faker import Faker
import csv
output=open("dirty-data.csv','w')
fake=Faker()
header=['guid','age','birthday','signup_date','account_type']
mywriter=csv.writer(output)
mywriter.writerow(header)
for r in range(1000):
    mywriter.writerow([fake.guid(),fake.random_int(min=18, max=80, step=1), fake.guid(), fake.age(),fake.birthday(),fake.signup_date(),fake.account_type())])
output.close()
