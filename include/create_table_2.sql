CREATE TABLE IF NOT EXISTS table_2 (
    customer_id TEXT,
    is_active BOOLEAN,
    date_activated DATE,
    date_churn DATE,
    active_deployments INT
);

TRUNCATE TABLE table_2;

INSERT INTO table_2
VALUES 
    ('c001', TRUE, '2022-08-01', NULL, 2),
    ('c002', TRUE, '2022-08-02', NULL, 7),
    ('c003', TRUE, '2022-07-02', NULL, 13),
    ('c004', FALSE, '2022-08-03', '2022-09-01', 0),
    ('c005', FALSE, NULL, NULL, 0);
    