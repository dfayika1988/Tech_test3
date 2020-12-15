from faker import Faker
import json
output=open('dirty-data.json','w')
fake=Faker()
alldata={}
alldata['records']=[]
for x in range(1000):
	data={"name":fake.name(),"age":fake.random_int(min=18, max=80, step=1),"guid":fake.guid(),"birthday":fake.birthday()," signup_date":fake. signup_date(),"account_type":fake.account_type())}
	alldata['records'].append(data)	
json.dump(alldata,output)
