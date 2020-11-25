package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Commands {
    public static void parseUserCommand(String userCommand) {

        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
        switch (commandTokens.get(0)) {
            case "show":
                System.out.println("Show");
                parseUserCommand("Select * from davisbase_tables");
                break;
            case "select":
                System.out.println("Select");
                parseSelectQuery(userCommand);
                break;
            case "drop":
                System.out.println("Drop");
                parseDropTable(userCommand);
                break;
            case "create":
                System.out.println("Create");
                if (commandTokens.get(1).equals("table"))
                    parseCreateTable(userCommand);
                else if (commandTokens.get(1).equals("index"))
                    parseCreateIndex(userCommand);
                break;
            case "insert":
                System.out.println("Insert");
                parseInsertTable(userCommand);
                break;
            case "update":
                System.out.println("Update");
                parseUpdateTable(userCommand);
            case "delete":
                System.out.println("Delete");
                parseDeleteTable(userCommand);
                break;
            case "help":
                Statements.help();
                break;
            case "version":
                Statements.version();
                break;
            case "exit":
            case "quit":
                Settings.setExit(true);
                break;
            default:
                Statements.errorCommand(userCommand);
                break;
        }
    }
    
    public static void parseCreateTable(String command) {

		// TODO: Before attempting to create new table file, check if the table already exists
		
		System.out.println("Stub: parseCreateTable method");
		System.out.println("Command: " + command);

        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        
        if (!createTableTokens.get(1).equals("table")) {
            System.out.println(Statements.SYNTAX_ERROR);
            return;
        }
        String tableName = createTableTokens.get(2);

        if (tableName.trim().length() == 0) {
            System.out.println(Statements.TABLE_NAME_EMPTY);
            return;
        }
        try {

        } catch (Exception e) {

            System.out.println(Statements.ERROR_CREATING_TABLE);
            System.out.println(e.getMessage());
        }
	}

    public static void parseDeleteTable(String command) {
        ArrayList<String> deleteTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));

        String tableName = "";

        try {

            if (!deleteTableTokens.get(1).equals("from") || !deleteTableTokens.get(2).equals("table")) {
                System.out.println(Statements.SYNTAX_ERROR);
                return;
            }

            tableName = deleteTableTokens.get(3);

        }
        catch (Exception e) {
            System.out.println("Error on deleting rows in table : " + tableName);
            System.out.println(e.getMessage());
        }
    }

    public static void parseInsertTable(String command) {
        ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));

        if (!insertTokens.get(1).equals("into") || !command.contains(") values")) {
            System.out.println(Statements.SYNTAX_ERROR);
            return;
        }
        try {
            String tableName = insertTokens.get(2);
            if (tableName.trim().length() == 0) {
                System.out.println(Statements.TABLE_NAME_EMPTY);
                return;
            }
        } catch (Exception ex) {
            System.out.println(Statements.ERROR_INSERTING);
            System.out.println(ex);
        }
    }

    public static void parseDropTable(String command) {
        String[] tokens = command.split(" ");
        if (!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
            System.out.println(Statements.SYNTAX_ERROR);
            return;
        }

        ArrayList<String> dropTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        String tableName = dropTableTokens.get(2);
        System.out.println(tableName);
    }

    public static void parseSelectQuery(String command) {
        String table_name = "";
        List<String> column_names = new ArrayList<String>();

        // Get table and column names for the select
        ArrayList<String> queryTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        int i = 0;

        for (i = 1; i < queryTableTokens.size(); i++) {
            if (queryTableTokens.get(i).equals("from")) {
                ++i;
                table_name = queryTableTokens.get(i);
                break;
            }
            if (!queryTableTokens.get(i).equals("*") && !queryTableTokens.get(i).equals(",")) {
                if (queryTableTokens.get(i).contains(",")) {
                    ArrayList<String> colList = new ArrayList<String>(
                            Arrays.asList(queryTableTokens.get(i).split(",")));
                    for (String col : colList) {
                        column_names.add(col.trim());
                    }
                } else
                    column_names.add(queryTableTokens.get(i));
            }
        }
        System.out.println(table_name);
    }

    public static void parseCreateIndex(String command) {
        ArrayList<String> createIndexTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        try {
            if (!createIndexTokens.get(2).equals("on") || !command.contains("(")
                    || !command.contains(")") && createIndexTokens.size() < 4) {
                System.out.println(Statements.SYNTAX_ERROR);
                return;
            }

            String tableName = command
                    .substring(command.indexOf("on") + 3, command.indexOf("(")).trim();
            String columnName = command
                    .substring(command.indexOf("(") + 1, command.indexOf(")")).trim();

            System.out.println(tableName);
            System.out.println(columnName);
            
        } catch (Exception e) {

            System.out.println(Statements.INDEX_ERROR);
            System.out.println(e);
        }

    }

    public static void parseUpdateTable(String command) {
        String[] tokens = command.split(" ");
        if (!(tokens[0].trim().equalsIgnoreCase("UPDATE") && tokens[2].trim().equalsIgnoreCase("SET"))) {
            System.out.println(Statements.SYNTAX_ERROR);
            return;
        }
    }

}
