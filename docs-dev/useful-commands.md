# Cleanup

* Kill all running mongod processes
```
killall mongod
```
* Remove all data from a mongo instance
```
mongosh <URI>
var dbs = db.getMongo().getDBNames()
for (var i in dbs) { db = db.getMongo().getDB(dbs[i]); print("dropping db " + db.getName()); (!['admin','config','local'].includes(db.getName())) && db.dropDatabase(); }
```
* Remove metadata associated with the flow
```
./dsync ... --cleanup
```