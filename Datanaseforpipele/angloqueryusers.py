import psycopg2 as db
conn_string="dbname='anglo' host='localhost' user='Kaka22' password='Kaka22'"
conn=db.connect(conn_string)
cur=conn.cursor()
query = "select * from users"
cur.execute(query)
print(cur.fetchone())
print(cur.rowcount)
print(cur.rownumber)
print(cur.fetchmany(3))
print(cur.rownumber)
f=open('data-dirty.csv','w')
conn=db.connect(conn_string)
cur=conn.cursor()
cur.copy_to(f,'users',sep=',')
f.close()
f=open('data-dirty.csv','r')
print(f.read())


