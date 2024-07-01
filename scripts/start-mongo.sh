#!/bin/bash
#
# Copyright (C) 2024 Adiom, Inc.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

echo "Detected hostname: $HOSTNAME"

mongod --replSet $REPL_SET_NAME --port 27017 --bind_ip_all --logpath /data/db/mongo.log --logappend &

wait_for_mongo() {
    while ! mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
        echo "Waiting for MongoDB to start..."
        sleep 2
    done
}

echo "Waiting for MongoDB to fully start..."
wait_for_mongo

echo "MongoDB started..."

if [ "$HOSTNAME" = "mongo1" ]; then
  echo "Initializing replica set for mongo1."
  mongosh --eval "
  try {
      if (rs.status().myState === 1) {
          print('mongo1 is already the primary.');
          print('Skipping data generation.');
          quit();
      }
  } catch (e) {
      print('No replica set configuration exists, initiating...');
      rs.initiate({
              _id: 'data1',
              members: [{ _id: 0, host: 'mongo1:27017', priority: 1 }]
          });
      while (rs.status().myState !== 1) {
          print('Waiting for mongo1 to become primary...');
          sleep(1000);
      }
      print('mongo1 is now the primary.');
      print('Generating random data...');
      var db = connect('mongodb://localhost:27017/testSeedDB');
      var docs = [];
      for (var i = 0; i < 4444; i++) {
          docs.push({
              name: 'User' + i,
              email: 'user' + i + '@example.com',
              age: Math.floor(Math.random() * 60) + 18,
              registered: new Date()
          });
      }
      db.myCollection.insertMany(docs);
      print('Random data generated successfully.');
  }
  "
elif [ "$HOSTNAME" = "mongo2" ]; then
  echo "Initializing replica set for mongo2 if needed and waiting for completion..."
  mongosh --eval "
  try {
      if (rs.status().myState === 1) {
          print('mongo2 is already the primary.');
          quit();
      }
  } catch (e) {
      print('No replica set configuration exists, initiating...');
      rs.initiate({
          _id: 'data2',
          members: [{ _id: 0, host: 'mongo2:27017', priority: 1 }]
      });
      while (rs.status().myState !== 1) {
          print('Waiting for mongo2 to become primary...');
          sleep(1000);
          waitCount++;
      }
      print('mongo2 is now the primary.');
  }
  "
  echo "Cleaning up all databases on mongo2..."
  mongosh --eval "
  var dbs = db.getMongo().getDBNames();
  for (var i in dbs) {
    var currentDb = db.getMongo().getDB(dbs[i]);
    if (!['admin', 'config', 'local'].includes(currentDb.getName())) {
      print('Dropping database: ' + currentDb.getName());
      currentDb.dropDatabase();
    }
  }
  "
fi

wait
