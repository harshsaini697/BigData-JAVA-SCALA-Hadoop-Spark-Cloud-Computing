M = LOAD '$M' USING PigStorage(',') AS (i,j,v);
N = LOAD '$N' USING PigStorage(',') AS (i,j,v);

join1 = JOIN M BY j, N BY i;
multiply1 = FOREACH join1 GENERATE M::i AS i,N::j AS j,(M::v)*(N::v) AS v;
group1 = GROUP multiply1 BY (i, j);
add = FOREACH group1 GENERATE $0 as i, SUM(multiply1.v) AS v;
STORE add INTO '$O' USING PigStorage(',');
dump add

