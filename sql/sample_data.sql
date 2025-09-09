
-- Seed example dimension data (optional for demo)
MERGE dbo.Customers AS target
USING (VALUES
    ('C001', 'Aisha Khan', 'aisha.khan@example.com'),
    ('C002', 'John Smith', 'john.smith@example.com'),
    ('C003', 'Sara Lee', 'sara.lee@example.com'),
    ('C004', 'Duplicate Guy', 'dup@example.com')
) AS src(CustomerId, Name, Email)
ON (target.CustomerId = src.CustomerId)
WHEN MATCHED THEN UPDATE SET Name = src.Name, Email = src.Email
WHEN NOT MATCHED THEN INSERT (CustomerId, Name, Email) VALUES (src.CustomerId, src.Name, src.Email);

MERGE dbo.Transactions AS target
USING (VALUES
    ('T1001', 'C001', 120.50, '2025-08-01 10:15:00'),
    ('T1002', 'C002', 89.99,  '2025-08-02 12:30:00'),
    ('T1003', 'C003', 0,      '2025-08-03 08:05:00'),
    ('T1004', 'C004', 45.00,  '2025-08-04 14:22:00')
) AS src(TransactionId, CustomerId, Amount, CreatedAt)
ON (target.TransactionId = src.TransactionId)
WHEN MATCHED THEN UPDATE SET CustomerId = src.CustomerId, Amount = src.Amount, CreatedAt = src.CreatedAt
WHEN NOT MATCHED THEN INSERT (TransactionId, CustomerId, Amount, CreatedAt) VALUES (src.TransactionId, src.CustomerId, src.Amount, src.CreatedAt);
