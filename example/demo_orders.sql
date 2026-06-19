CREATE  temporary view orders AS
SELECT * FROM VALUES
  (1, 'alice', 100, '2025-01-15'),
  (2, 'bob',   250, '2025-02-20'),
  (3, 'alice', 150, '2025-03-10'),
  (4, 'bob',   300, '2025-04-05')
AS t(order_id, user_name, amount, order_date);


CREATE  temporary view orders2 AS
SELECT * FROM VALUES
  (1, 'alice', 101, '2025-01-14'),  -- slightly different data to test assertion failures
  (2, 'bob',   250, '2025-02-20'),
  (3, 'alice', 150, '2025-03-10'),
  (4, 'bob',   300, '2025-04-05')
AS t(order_id, user_name, amount, order_date);


-- @test order_stats
-- @assert_all amount > 0
-- @assert_not_empty
-- @assert_unique order_id
SELECT order_id, user_name, amount, order_date
FROM orders;


-- @test order_total
-- @dependency order_stats
-- @assert_agg_equal sum amount
SELECT user_name, amount
FROM orders
WHERE user_name = 'alice';

SELECT user_name, amount
FROM orders2
WHERE user_name = 'alice';


-- @test compare_users
-- @assert_join_numeric_ratio_approx 0.001 on user_name values total
SELECT user_name, sum(amount) total
FROM orders
group by user_name;

SELECT user_name, sum(amount) total
FROM orders2
group by user_name
