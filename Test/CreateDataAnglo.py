from faker import Faker
import json
import os
os.chdir("/home/demilsonfayika/dataanglo")
fake=Faker()
userid=1
for i in range(1000):
    name=fake.guid()
    fname=name.replace(" ","-")+'.json'
    data={
        "guid":guid,
        "age":fake.random_int(min=18, max=101, step=1),
        "birthday":fake.(),
        "signup_date":fake.signup_date(),
        "account_type":fake.account_type()
    }
    datajson=json.dumps(data)


    output=open(fname,'w')
    userid+=1
    output.write(datajson)
    output.close()
