from elasticsearch import Elasticsearch
from elasticsearch import helpers
from faker import Faker

fake=Faker()
es = Elasticsearch() #or pi {127.0.0.1}

actions = [
  {
    "_index": "users",
    "_type": "doc",
    "_source": {
	"guid": fake.guid(),
	"age": fake.age(), 
	"birthday": fake.birthday(),
	"signup_date":fake.signup_date(),
       "account_type":fake.account_type}
  }
  for x in range(998) # or for i,r in df.iterrows()
]

response = helpers.bulk(es, actions)
print(response)

