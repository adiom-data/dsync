# Docker Commands

This document outlines various Docker commands useful for setting up, running, and managing the development environment.

## Initial Setup and First Run

```sh
docker compose up --build
```

## Clean Start

```sh
docker compose down -v
docker compose up --build
```

## Accessing MongoDB Instances

```sh
docker exec -it dsync-mongo1-1 mongosh
docker exec -it dsync-mongo2-1 mongosh
```

## Cleaning the MongoDB Instances

```sh
docker exec -it dsync-mongo2-1 mongosh
var dbs = db.getMongo().getDBNames()
for (var i in dbs) { db = db.getMongo().getDB(dbs[i]); print("dropping db " + db.getName()); (!['admin','config','local'].includes(db.getName())) && db.dropDatabase(); }
```


## Building After Code Changes

Currently, there is no hot reloading, so you must rebuild the containers after any code changes

```sh
docker compose up --build
```
