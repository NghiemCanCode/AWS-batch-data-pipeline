This folder stores SQL scripts for creating and maintaining the structure of base tables (fact and dimension).

CI/CD pipelines will **not** be applied to these scripts because the creation and modification of table structures should be performed manually, following a standardized and approved process.  
This ensures data consistency with the source systems and prevents unintended schema changes in production.

**Notes / Guidelines:**
- All structural changes must be reviewed and approved by the data engineering lead (or DBA).
- After making changes, update corresponding documentation (e.g., data dictionary, ERD).
- Version control (Git) should still be used to track changes to these scripts.
- Apply these scripts only in controlled environments (e.g., via migration windows or change requests).
