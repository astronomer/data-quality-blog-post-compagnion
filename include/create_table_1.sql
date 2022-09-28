CREATE TABLE IF NOT EXISTS table_1 (
    customer_date_id TEXT,
    customer_id TEXT,
    date DATE,
    dag_runs INT,
    successful_tasks INT,
    failed_tasks INT,
    total_tasks INT,
    success_rate FLOAT
);

TRUNCATE TABLE table_1;

INSERT INTO table_1
VALUES  
    ('c001d20220901', 'c001', '2022-09-01', 23, 90, 10, 100, 0.9),
    ('c002d20220902', 'c002', '2022-09-02', 42, 6430, 0, 6430, 1),
    ('c001d20220902', 'c001', '2022-09-02', 9, 2250, 0, 2250, 1),
    ('c002d20220903', 'c002', '2022-09-03', 7, 990, 10, 1000, 0.99),
    ('c003d20220903', 'c003', '2022-09-03', 19, 1200, 0, 1200, 1);