#!/bin/bash
set -e

# Record overall start time
OVERALL_START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
OVERALL_START_TIMESTAMP=$(date +%s)

echo "🕐 Starting sequential TPCH sync tasks at: $OVERALL_START_TIME"

# Task 0: Create MongoDB indexes for optimal performance
echo "🔧 Creating MongoDB indexes for embedding operations..."
INDEX_START=$(date +%s)

mongosh ${DST} --eval "
console.log('Creating indexes for TPCH embedding operations...');

// Switch to your database (adjust database name as needed)
db = db.getSiblingDB('public');  

// Orders collection indexes (for customer and lineitem embedding)
try {
  db.orders.createIndex({'o_custkey': 1}, {name: 'idx_custkey'});
  console.log('✅ Created idx_custkey on orders collection');
} catch(e) { console.log('⚠️  idx_custkey already exists or failed:', e.message); }

try {
  db.part.createIndex({'_id': 1, 'suppliers._id': 1}, {name: 'idx_part_suppliers'});
  console.log('✅ Created idx_part_suppliers on part collection');
} catch(e) { console.log('⚠️  idx_part_suppliers already exists or failed:', e.message); }

try {
  db.orders.createIndex({'lineitems._id': 1}, {sparse: true, name: 'idx_lineitems'});
  console.log('✅ Created idx_lineitems on orders collection');
} catch(e) { console.log('⚠️  idx_lineitems already exists or failed:', e.message); }

try {
    db.supplier.createIndex({'s_nationkey': 1}, {name: 'idx_nationkey'});
    console.log('✅ Created idx_nationkey on supplier collection');
} catch(e) { console.log('⚠️  idx_nationkey already exists or failed:', e.message); }

try {
    db.supplier.createIndex({'nation.n_regionkey': 1}, {name: 'idx_regionkey'});
    console.log('✅ Created idx_regionkey on supplier collection');
} catch(e) { console.log('⚠️  idx_regionkey already exists or failed:', e.message); }   

try {
    db.supplier.createIndex({'_id': 1, 'parts._id': 1}, {name: 'idx_supplier_parts'});
    console.log('✅ Created idx_supplier_parts on supplier collection');
} catch(e) { console.log('⚠️  idx_supplier_parts already exists or failed:', e.message); }

console.log('Index creation completed.');
"
INDEX_END=$(date +%s)
INDEX_DURATION=$((INDEX_END - INDEX_START))
echo "✅ Index creation completed! Duration: $INDEX_DURATION seconds ($(date -u -r $INDEX_DURATION '+%H:%M:%S'))"
echo ""

#Task 1: Large tables group (part, partsupp, supplier, orders, lineitem)
echo "🚀 Starting Task 1: Large tables group..."
TASK1_START=$(date +%s)
../dsync --namespace "public.partsupp,public.orders,public.lineitem" --mode "Snapshot" $SRC $DST grpc://localhost:8086 --insecure

TASK1_END=$(date +%s)
TASK1_DURATION=$((TASK1_END - TASK1_START))
echo "✅ Task 1 completed! Duration: $TASK1_DURATION seconds ($(date -u -r $TASK1_DURATION '+%H:%M:%S'))"
echo ""

# Task 2: Customer/Nation tables
echo "🚀 Starting Task 2: Customer and Nation tables..."
TASK2_START=$(date +%s)
../dsync --namespace "public.customer, public.nation, public.part, public.supplier" --mode "Snapshot" $SRC $DST grpc://localhost:8086 --insecure

TASK2_END=$(date +%s)
TASK2_DURATION=$((TASK2_END - TASK2_START))
echo "✅ Task 2 completed! Duration: $TASK2_DURATION seconds ($(date -u -r $TASK2_DURATION '+%H:%M:%S'))"
echo ""

# Task 3: Region table
echo "🚀 Starting Task 3: Region table..."
TASK3_START=$(date +%s)
../dsync --namespace "public.region" --mode "Snapshot" $SRC $DST grpc://localhost:8086 --insecure

TASK3_END=$(date +%s)
TASK3_DURATION=$((TASK3_END - TASK3_START))
echo "✅ Task 3 completed! Duration: $TASK3_DURATION seconds ($(date -u -r $TASK3_DURATION '+%H:%M:%S'))"
echo ""

# Calculate overall duration
OVERALL_END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
OVERALL_END_TIMESTAMP=$(date +%s)
OVERALL_DURATION=$((OVERALL_END_TIMESTAMP - OVERALL_START_TIMESTAMP))

echo "🎉 All sequential sync tasks completed successfully!"
echo ""
echo "📊 TIMING SUMMARY:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Overall Start Time:  $OVERALL_START_TIME"
echo "Overall End Time:    $OVERALL_END_TIME"
echo "Total Duration:      $OVERALL_DURATION seconds ($(date -u -r $OVERALL_DURATION '+%H:%M:%S'))"
echo ""
echo "Task 1 Duration:     $TASK1_DURATION seconds ($(date -u -r $TASK1_DURATION '+%H:%M:%S')) - Large tables"
echo "Task 2 Duration:     $TASK2_DURATION seconds ($(date -u -r $TASK2_DURATION '+%H:%M:%S')) - Customer/Nation"
echo "Task 3 Duration:     $TASK3_DURATION seconds ($(date -u -r $TASK3_DURATION '+%H:%M:%S')) - Region"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"