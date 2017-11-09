#!/usr/bin/env python2

from cassandra.cluster import Cluster
print "Cluster imported"

cluster = Cluster(['10.0.2.15', '10.0.2.14'])
print "cluster obj created"

session = cluster.connect('debkey1')
print "session to connect to keyspace initiated"

rows = session.execute('SELECT * FROM debemp2')
print "Fetching of rows from table..."

for everyrow in rows:
	print "{} - {} - {} ".format(everyrow.empid,everyrow.empname,everyrow.empsal)
print "Insert started..."

session.execute('insert into debemp2 (empname , empsal , empid ) VALUES(%s,%s,%s)', ('Gelu',20000, 4))
session.execute('insert into debemp2 (empname , empsal , empid ) VALUES(%s,%s,%s)', ('Kuny Doggy',25000, 5))
session.execute('insert into debemp2 (empname , empsal , empid ) VALUES(%s,%s,%s)', ('Baba',30000, 6))

print "Insert Done" 

rows = session.execute('SELECT * FROM debemp2')
print "Fetching of rows from table..."

for everyrow in rows:
	print "{} - {} - {} ".format(everyrow.empid,everyrow.empname,everyrow.empsal)

print "Done"
