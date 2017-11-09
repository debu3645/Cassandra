#!/usr/bin/env python2

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.policies import HostDistance, RetryPolicy, RoundRobinPolicy, DCAwareRoundRobinPolicy

print "Cluster imported"

#cluster = Cluster(['10.0.2.15', '10.0.2.14'])
#cluster = Cluster(contact_points=['10.0.2.16','10.0.2.17','10.0.2.15', '10.0.2.14'], port=9042, load_balancing_policy= RoundRobinPolicy())
cluster = Cluster(contact_points=['10.0.2.16','10.0.2.15', '10.0.2.14'], port=9042, load_balancing_policy= DCAwareRoundRobinPolicy(local_dc='datacenter1'))
#For simplesnitch the correct value should be local_dc='datacenter1'
print "cluster obj created"

session = cluster.connect('debkey3')
print "session to connect to keyspace initiated"

rows = session.execute('SELECT * FROM debemp3')
print "Fetching of rows from table..."

for everyrow in rows:
	print "{} - {} - {} ".format(everyrow.empid,everyrow.empname,everyrow.empsal)
print "Insert started..."

session.execute('insert into debemp3 (empname , empsal , empid ) VALUES(%s,%s,%s)', ('Gelu',20000, 1))
session.execute('insert into debemp3 (empname , empsal , empid ) VALUES(%s,%s,%s)', ('Kuny Doggy',25000, 2))
session.execute('insert into debemp3 (empname , empsal , empid ) VALUES(%s,%s,%s)', ('Dhanamali',30000, 3))
session.execute('insert into debemp3 (empname , empsal , empid ) VALUES(%s,%s,%s)', ('Drishti',35000, 4))
session.execute('insert into debemp3 (empname , empsal , empid ) VALUES(%s,%s,%s)', ('Chhotu',40000, 5))

print "Insert Done" 

rows = SimpleStatement('SELECT * FROM debemp3', consistency_level=ConsistencyLevel.QUORUM)
#rows = SimpleStatement('SELECT * FROM debemp3', consistency_level=ConsistencyLevel.LOCAL_ONE)
#rows = SimpleStatement('SELECT * FROM debemp3', consistency_level=ConsistencyLevel.ONE)
myrows = session.execute(rows)
print "myrows: ", myrows
print "Fetching of rows from table using 'Consistency-Level=Quorum'..."

for everyrow in session.execute(rows):
	print "{} - {} - {} ".format(everyrow.empid,everyrow.empname,everyrow.empsal)

# To iterate simplestatement we must use session.execute(rows). Otherwise it doesnt work.
print "Done"
