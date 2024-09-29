mongosh --port $PORT --eval "db.getMongo().getDBNames().filter(d => !['admin','local','config'].includes(d)).forEach(d => db.getMongo().getDB(d).dropDatabase());"
