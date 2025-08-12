# Mapping Transform

### Run Postgres and MongoDB servers

Using Postgres schema from https://github.com/simonekudia/ExploringRelationalMigrator

Mapping customer, staff, and inventory collection to store collection

Clone Repo and run

```
docker compose up postgres mongodb
```

Run the following:
```
export SRC="postgresql://postgres:123456@localhost:5432/pagila?sslmode=disable"
export DST="mongodb://localhost:27017"

./dsync --namespace "public.store" --verbosity DEBUG $SRC $DST grpc://localhost:8086 --insecure
./dsync --namespace "public.customer,public.staff,public.inventory" --verbosity DEBUG $SRC $DST grpc://localhost:8086 --insecure
```

tpch dataset:

```
export SRC="postgresql://postgres:123456@localhost:5433/tpch?sslmode=disable"

mongod --dbpath data_d --logpath mongo_d.log --fork --port 27017

export DST="mongodb://localhost:27017"

./dsync --namespace "public.part,public.partsupp,public.supplier,public.orders,public.lineitem" $SRC $DST grpc://localhost:8086 --insecure

./dsync --namespace "public.customer, public.nation" $SRC $DST grpc://localhost:8086 --insecure

./dsync --namespace "public.region" --verbosity DEBUG $SRC $DST grpc://localhost:8086 --insecure
```


