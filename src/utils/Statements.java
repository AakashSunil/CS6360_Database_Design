package utils;

public class Statements {
    public static final String EXIT = "Safely Exited from DavisBase.";
    public static final String SYNTAX_ERROR = "Syntax Error";
    public static final String TABLE_NAME_EMPTY = "Table Name Cannot be Empty";
    public static final String ERROR_INSERTING = "Error while Inserting";
    public static final String ERROR_CREATING_TABLE = "Error Creating Table";
    public static final String INDEX_ERROR = "Error Creating Index";
    public static final String INVALID_COMMAND = "Invalid Command: \"";



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

        System.out.println("help;");
        System.out.println("Lists the available commands.\n");
        System.out.println("create table <table_name> (<column_list>);");
        System.out.println("Creates a table with the table name and the column list. No datatype is needed to be specified as all data is considered as string in thos database.\n");
        System.out.println("insert into table <table_name> values (<values_list>);");
        System.out.println("Inserts a record into the table. The values_list needs to be in the same order as the columns when the table was created.\n");
        System.out.println("select <column_names> from <table_name> where <condition>;");
        System.out.println("Displays the values of the column names specified as per condition. Condition works only on equality. Condition is Mandatory.\n");
        System.out.println("show tables;");
        System.out.println("Lists the available table names in the database in an array format.\n");
        System.out.println("drop table <table_name>;");
        System.out.println("Drops/Deletes the table from the database.\n");
        System.out.println("delete from table <table_name> where <condition>;");
        System.out.println("Deletes record from table based on the single condition.\n");
        System.out.println("update <table_name> set <column_name> = <value> where <condition>;");
        System.out.println("Updates the specific column based on the where condition. Only single column value and single equality condition.\n");
        System.out.println("exit;");
        System.out.println("Exits the Database safely.\n");

        System.out.println(printSeparator("-", 90));
    }

    public static void version() {
        System.out.println(Settings.getVersion());
    }

    public static void errorCommand(String command) {
        System.out.println(Statements.INVALID_COMMAND + command + "\"");
    }
}