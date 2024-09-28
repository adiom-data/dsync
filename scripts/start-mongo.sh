#!/bin/bash
#
# Copyright (C) 2024 Adiom, Inc.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
echo "Detected hostname:port: $HOSTNAME:$PORT"

mongod --replSet $REPL_SET_NAME --port $PORT --bind_ip_all --logpath /data/db/mongo.log --logappend &

wait_for_mongo() {
    while ! mongosh --port $PORT --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
        echo "Waiting for MongoDB to start..."
        sleep 2
    done
}

echo "Waiting for MongoDB to fully start..."
wait_for_mongo

echo "MongoDB started..."

echo "Initializing replica set for mongo1."
mongosh --port $PORT --eval "
try {
    if (rs.status().myState === 1) {
        print('$HOSTNAME is already the primary.');
        print('Skipping data generation.');
        quit();
    }
} catch (e) {
    print('No replica set configuration exists, initiating...');
    rs.initiate({
        _id: '$REPL_SET_NAME',
        members: [{ _id: 0, host: '$HOSTNAME:$PORT', priority: 1 }]
        });
    while (rs.status().myState !== 1) {
        print('Waiting for $HOSTNAME to become primary...');
        sleep(1000);
    }
    print('$HOSTNAME is now the primary.');
}
"

wait
