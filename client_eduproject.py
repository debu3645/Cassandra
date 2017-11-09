#!/usr/bin/env python2

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.policies import HostDistance, RetryPolicy, RoundRobinPolicy, DCAwareRoundRobinPolicy
#from cassandra.encoder import cql_encode_unicode
import sys
reload(sys)
sys.setdefaultencoding('utf8')   # If this is not passed, Cassandra fails to extract special characters from the database

print "Cluster imported"

#cluster = Cluster(contact_points=['10.0.2.16','10.0.2.17','10.0.2.15', '10.0.2.14'], port=9042, load_balancing_policy= RoundRobinPolicy())
cluster = Cluster(contact_points=['10.0.2.16','10.0.2.15', '10.0.2.14'], port=9042, load_balancing_policy= DCAwareRoundRobinPolicy(local_dc='datacenter1'))
#For simplesnitch the correct value should be local_dc='datacenter1'
print "cluster obj created"

session = cluster.connect('playlist')
print "session to connect to keyspace initiated"

rows = session.execute("SELECT * FROM artists_by_first_letter where first_letter = 'D'")
print "Fetching of rows from table..."

for everyrow in rows:
	print "{} - {} ".format(str(everyrow.first_letter),str(everyrow.artist))

print "Simple fetching of records done..." 
print "----------------------------------"
print "----------------------------------"
print "Fetching records with Consistency-Level starts..."

rows = SimpleStatement('SELECT * FROM artists_by_first_letter', consistency_level=ConsistencyLevel.QUORUM)
#rows = SimpleStatement('SELECT * FROM artists_by_first_letter', consistency_level=ConsistencyLevel.LOCAL_ONE)
#rows = SimpleStatement('SELECT * FROM artists_by_first_letter', consistency_level=ConsistencyLevel.ONE)
myrows = session.execute(rows)
print "myrows: ", myrows
print "Fetching of rows from table using 'Consistency-Level=Quorum'..."

for everyrow in session.execute(rows):
	print "{} - {} ".format(everyrow.artist,everyrow.first_letter)

# To iterate simplestatement we must use session.execute(rows). Otherwise it doesnt work.
print "Done"
print "----------------------------------"
print "----------------------------------"
print "----------------------------------"
print "Insert operation started...."
session.execute('insert into artists_by_first_letter (first_letter , artist ) VALUES(%s,%s)', ('D', 'Drishti/Darshini Pattnaik'))
session.execute('insert into artists_by_first_letter (first_letter  , artist ) VALUES(%s,%s)', ('D', 'Debashish ** Pattnaik'))
session.execute('insert into artists_by_first_letter (first_letter  , artist ) VALUES(%s,%s)', ('R', 'Rupali / Jhumuri / Singh'))
session.execute('insert into artists_by_first_letter (first_letter  , artist ) VALUES(%s,%s)', ('C', 'Chhotu/NiuNiu****'))
session.execute('insert into artists_by_first_letter (first_letter  , artist ) VALUES(%s,%s)', ('G', 'Gelu/Drishti/Chhotu/Darshini^-^'))
print "Insert operation done...."
print "----------------------------------"
print "----------------------------------"
print "----------------------------------"
newrows = SimpleStatement('SELECT * FROM artists_by_first_letter', consistency_level=ConsistencyLevel.LOCAL_ONE)
myrows = session.execute(newrows)
print "Fetching of rows from table using 'Consistency-Level=Local'after inserting..."

for everyrow in session.execute(rows):
	print "{} - {} ".format(everyrow.artist,everyrow.first_letter)
print "----------------------------------"
print "----------------------------------"
print "----------------------------------"
print "----------------------------------"
print "----------------------------------"
