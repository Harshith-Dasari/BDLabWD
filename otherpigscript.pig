-- Load data from local filesystem
D = LOAD 'file:///home/cloudera/data1.txt' USING PigStorage(',') AS (a1:int, a2:int, a3:int);
DD = LOAD 'file:///home/cloudera/data2.txt' USING PigStorage(',') AS (b1:int, b2:int);

-- Debug: Check if data is loaded
DUMP D;
DUMP DD;

-- Convert keys to ensure correct data types
D_casted = FOREACH D GENERATE (int)a1 AS a1, a2, a3;
DD_casted = FOREACH DD GENERATE (int)b1 AS b1, b2;

-- Debug: Check transformed data
DUMP D_casted;
DUMP DD_casted;

-- Perform JOIN operation
X = JOIN D_casted BY a1, DD_casted BY b1;

-- Debug: Check JOIN results
DUMP X;

-- Perform LEFT OUTER JOIN to debug missing matches
X_left = JOIN D_casted BY a1 LEFT OUTER, DD_casted BY b1;
DUMP X_left;

-- Debug UNION operation
U = UNION D_casted, DD_casted;
DUMP U;

-- Store output in local filesystem for verification
STORE X INTO 'file:///home/cloudera/debug_join_output' USING PigStorage(',');
STORE U INTO 'file:///home/cloudera/debug_union_output' USING PigStorage(';');
