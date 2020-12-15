import psycopg2 as db
conn_string="dbname='anglo' host='localhost' user='postgres' password='postgres'"
conn=db.connect(conn_string)
cur=conn.cursor()
query = "insert into users (guid,age,birthday,signup_date,account_type) values({},'{}','{}','{}','{}')".format(1,'Big Bird','Sesame Street','Fakeville','12345')
print(cur.mogrify(query))
query2 = "insert into users (guid,age,birthday,signup_date,account_type) values(%s,%s,%s,%s,%s)"
data=(1,'a18347e0-3dcc-537e-9f0e-b24a79c37cfe','age','singup-date','account_type')
print(cur.mogrify(query2,data))
cur.execute(query2,data)
conn.commit()



