# CS6360_Database_Design

## How to run

To run the program (Assumption - Java is set up in the path)

Through Command Prompt

1. Open command prompt at the project location where the main application (Application.java) is present.
2. Run javac Application.java to compile the code.
3. Run java Application to run the complied code.

Through VSCode

1. Press F5 to run the code and open debug mode.

## SQL Commands


SQL Query Commands Syntax

1. Show Tables Command

	```sql
	show tables;
	```

	Lists the table names in an array format

2. Select Query

	```sql
	select <column_names> from <table_name> where <condition>;
	```

	Displays the values of the column names specified as per condition. Condition works only on equality. Condition is Mandatory.

3. Insert Query

	```sql
	insert into table <table_name> values (<values_list>);
	```

	Inserts a record into the table. The values_list needs to be in the same order as the columns when the table was created.

4. Drop Query

	```sql
	drop table <table_name>;
	```

	Drops/Deletes the table from the database.

5. Create Query

	```sql
	create table <table_name> (<column_list>);
	```

	Creates a table with the table name and the column list. No datatype is needed to be specified as all data is considered as string in thos database.

6. Update Query

	```sql
	update <table_name> set <column_name> = <value> where <condition>;
	```

	Updates the specific column based on the where condition. Only single column value and single equality condition.

7. Delete Query

	```sql
	delete from table <table_name> where <condition>;
	```

	Deletes record from table based on the single condition. 

8. Help Query

	```sql
	Help;
	```


	Lists the available commands.

9. Exit

  ```sql
  Exit;
  ```

  Exits the Database safely.