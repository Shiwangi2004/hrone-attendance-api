# DECISIONS.md

1. **Indexes.**
We created unique indexes on `employees(emp_code)` and `attendance_logs(emp_code, date)` to enforce data integrity and eliminate race conditions. For queries, we added compound indexes on `attendance_logs(date, emp_code)` for pagination and `attendance_logs(date, late_minutes)` for leaderboard filtering. On `employees`, `(department, joined_on)` accelerates trend headcount checks. We rejected a standalone index on `status` due to low cardinality; compound indexing with `date` is far more selective.

2. **Punch-in race.**
Both concurrent requests derive the same attendance date and attempt an `insert_one`. MongoDB's unique index on `(emp_code, date)` serializes the write: exactly one insert commits (returning HTTP 201), while the second triggers a `DuplicateKeyError`, caught cleanly to return HTTP 409 without check-then-insert races.

3. **Ties.**
Using MongoDB's `$setWindowFields` with `$rank: {}` assigns standard competition ranking (1, 2, 2, 4). Filtering on `rank <= limit` includes all employees sharing the cutoff rank, legitimately returning more rows than `limit` to ensure tied employees are not arbitrarily excluded.

4. **Headcount.**
The aggregation pipeline starts from `employees` (matching `joined_on <= month_end`) and `$lookup`s attendance logs. Since `employees` drives the pipeline, employees with zero logs still contribute 1 to the department's `$sum: 1` headcount.

5. **One change for 100x data.**
We would maintain pre-aggregated daily/monthly summary collections updated asynchronously via MongoDB Change Streams, and shard `attendance_logs` on `{emp_code: "hashed", date: 1}` to horizontally distribute write load.
