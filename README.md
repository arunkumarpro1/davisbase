# davisbase

THis project implemenst a very rudimentary database engine that is loosely based on a hybrid between MySQL and SQLite, which I call DavisBase ([Davis](https://www.utdallas.edu/~chrisirwindavis/) is my Database Professor). The implementation operates entirely from the command line and API calls. The database supports actions on a single table at a time, no joins or nested queries. Like MySQL's InnoDB data engine (SDL), this program uses file-per-table approach to physical storage. Each database table is physcially stored as a separate file. Each table file is subdivided into logical sections of fixed equal size call pages. Therefore, each table file size will be exact increments of the global page_size attribute, i.e. all data files shares the same page_size attribute.

**Prompt**
----------
Upon launch, the db engine presents a prompt like:

davisql>

**Supported Commands**
----------------------

The database engine supports the following DDL, DML, and VDL commands. All commands should be terminated by a semicolon (;).

**DDL**

• SHOW TABLES – Displays a list of all tables in DavisBase.

• CREATE TABLE – Creates a new table schema, i.e. a new empty table.

• DROP TABLE table_name – Remove a table schema, and all of its contained data.

• No support for ALTER TABLE schema change commands.

**DML**

• INSERT INTO table_name [column_list] VALUES value_list – Inserts a single record into a table.

• DELETE FROM table_name [column_list] VALUES value_list – Inserts a single record into a table.

• UPDATE table_name SET column_name = value [WHERE condition] – Modifies one or more records in a table.

**VDL**

• “SELECT-FROM-WHERE” -style query
- No support for JOIN commands. All queries must be single table queries.
- No support for ORDER BY, GROUP BY, HAVING, or AS alias.

• EXIT – Cleanly exits the program and saves all table information in non-volatile files to disk
