


public class Statements {
    public static final String EXIT = "Safely Exited from DavisBase.";
    public static final String SYNTAX_ERROR = "Syntax Error";
    public static final String TABLE_NAME_EMPTY = "Table Name Cannot be Empty";
    public static final String ERROR_INSERTING = "Error while Inserting";
    public static final String ERROR_CREATING_TABLE = "Error Creating Table";
    public static final String INDEX_ERROR = "Error Creating Index";
    public static final String INVALID_COMMAND = "Invalid Command: \"";
    public static final String ROW_ID = "Row ID Included";
    public static final String EXISTING_INDEX = "Index already Existing";
    public static final String TABLE_NAME_INVALID = "Invalid Table Name";
    public static final String COLUMN_NAME_INVALID = "Invalid Column Name";
    public static final String CREATED_INDEX = "Index Created";
    public static final String TABLE_DELETED = "Table Deleted";
    public static final String TABLE_DOES_NOT_EXIST = "Table Does Not Exist";
    public static final String INDEX_DELETED = "Index Deleted";
    public static final String INDEX_DOES_NOT_EXIST = "Index Does Not Exist";
    public static final String ERROR_SELECT_COLUMN = "Error in Selecting Column";
    public static final String INSERT_ERROR = "Error inserting";
    public static final String TABLE_CREATE_ERROR = "Table Creation Failed";






    public static void splashScreen() {
		System.out.println(printSeparator("-",80));
	    System.out.println("Welcome to DavisBase");
		System.out.println("DavisBase Version " + Settings.getVersion());
		System.out.println(Settings.getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(printSeparator("-",80));
	}
    public static String printSeparator(String s, int len) {
		String bar = "";
		for(int i = 0; i < len; i++) {
			bar += s;
		}
		return bar;
	}
	public static void help() {
        System.out.println(printSeparator("-", 90));
        System.out.println("Available commands");

        System.out.println("-  HELP;");
        System.out.println("\tShows all the available help commands");
        System.out.println("-  CREATE TABLE <table_name> (<column_name>);");
        System.out.println("\tLets you create table in the database");
        System.out.println("-  INSERT INTO <table_name> (<column_list>) VALUES (<values_list>);");
        System.out.println("\tInsert appropriate values into given table. Column List order should be same as at the time of creation.");
        System.out.println("-  SELECT <column_list> FROM <table_name> [WHERE <condition>];");
        System.out.println("\tDisplay the values from the table with or without a condition");
        System.out.println("-  SHOW TABLES;");
        System.out.println("\tDisplay all the available tables in the database");
        System.out.println("-  DROP TABLE <table_name>;");
        System.out.println("\tDeletes table from the database");
        System.out.println("-  DELETE FROM TABLE <table_name> WHERE <condition>;");
        System.out.println("\tDeletes specific table row table from the database");
        System.out.println("-  UPDATE <table_name> SET <column_name> = <value> WHERE <condition>;");
        System.out.println("\tUpdates particular column value in table from the database based on the condition;");
        System.out.println("-  VERSION;");
        System.out.println("\t Displays the Version of the software");
        System.out.println("-  EXIT;");
        System.out.println("\tSafely EXIT the Database");

        System.out.println(printSeparator("-", 90));
    }

    public static void version() {
        System.out.println(Settings.getVersion());
    }

    public static void errorCommand(String command) {
        System.out.println(Statements.INVALID_COMMAND + command + "\"");
    }
}
