CREATE TABLE test(i INTEGER, j INTEGER, t TIMESTAMP TIME INDEX);

INSERT INTO test VALUES (1, 1, 1), (NULL, 1, 2), (1, NULL, 3);

SELECT * FROM test ORDER BY i NULLS FIRST, j NULLS LAST;

SELECT * FROM test ORDER BY i NULLS FIRST, j NULLS FIRST;

SELECT * FROM test ORDER BY i NULLS LAST, j NULLS FIRST;

-- Temporary disable. Waiting for next upgrade of DataFusion.
-- SELECT i, j, row_number() OVER (PARTITION BY i ORDER BY j NULLS FIRST) FROM test ORDER BY i NULLS FIRST, j NULLS FIRST;

SELECT i, j, row_number() OVER (PARTITION BY i ORDER BY j NULLS LAST) FROM test ORDER BY i NULLS FIRST, j NULLS FIRST;

SELECT * FROM test ORDER BY i NULLS FIRST, j NULLS LAST LIMIT 2;

SELECT * FROM test ORDER BY i NULLS LAST, j NULLS LAST LIMIT 2;

SELECT * FROM test ORDER BY i;

SELECT * FROM test ORDER BY i NULLS FIRST;

SELECT * FROM test ORDER BY i NULLS LAST;

DROP TABLE test;
