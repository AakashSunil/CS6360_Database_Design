package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import Storage.Database;

public class Commands {
    public static void parseUserCommand(String userCommand) {

        Database database = new Database();
        database.initial();
        database.createTable("Student",
                new ArrayList<>(Arrays.asList("name", "age", "gender")));
        database.insertRowIntoTable("Student",
                new ArrayList<>(Arrays.asList("John", "24", "male")));
        database.insertRowIntoTable("Student",
                new ArrayList<>(Arrays.asList("Taylor", "23", "male")));
        database.insertRowIntoTable("Student",
                new ArrayList<>(Arrays.asList("Alice", "23", "female")));
        database.deleteRowsFromTable("Student", "age", "24");
        database.updateRowsInTable("Student", "age", "25",
                "name", "Taylor");

        ArrayList<String> tableNameList = database.showTables();
        ArrayList<ArrayList<String>> queryResult = database.queryInTable(
                "Student",
                new ArrayList<>(Arrays.asList("gender", "name")),
                "gender",
                "male"
        );

        // database.exit();

        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
        switch (commandTokens.get(0)) {
            case "show":
                database.showTables();
                break;
            case "select":
                String[] results = parseSelectQuery(userCommand);
                System.out.println(results[0]+"\n"+new ArrayList<>(Arrays.asList(results[1].split(",")))+"\n"+results[2]+"\n"+results[3]);
                database.queryInTable(results[0],new ArrayList<>(Arrays.asList(results[1].split(","))),results[2],results[3]);
                break;
            case "drop":
                System.out.println("Drop");
                String result_drop = parseDropTable(userCommand);
                System.out.println(result_drop);
                database.dropTable(result_drop);
                break;
            case "create":
                System.out.println("Create");
                if (commandTokens.get(1).equals("table")) {
                    String[] results_create = parseCreateTable(userCommand);
                    if(results_create.length == 1) {
                        System.out.println(results_create[0]);
                    }
                    else {
                        System.out.println(results_create[0]+"\n"+new ArrayList<>(Arrays.asList(results_create[1].split(","))));
                        database.createTable(results_create[0], new ArrayList<>(Arrays.asList(results_create[1].split(","))));
                    }
                }
                else if (commandTokens.get(1).equals("index"))
                    parseCreateIndex(userCommand);
                break;
            case "insert":
                System.out.println("Insert");
                String[] result_insert = parseInsertTable(userCommand);
                if(result_insert.length == 1) {
                    System.out.println((result_insert[0]));
                }
                else {
                    System.out.println(result_insert[0]+"\n"+new ArrayList<>(Arrays.asList(result_insert[1].split(","))));
                    database.insertRowIntoTable(result_insert[0], new ArrayList<>(Arrays.asList(result_insert[1].split(","))));
                }
                break;
            case "update":
                System.out.println("Update");
                String[] result_update = parseUpdateTable(userCommand);
                if(result_update.length == 1) {
                    System.out.println(result_update[0]);
                }
                else {
                    System.out.println(result_update[0]+"\n"+ result_update[1]+"\n"+ result_update[2]+"\n"+ result_update[3]+"\n"+ result_update[4]);
                    database.updateRowsInTable(result_update[0], result_update[1],result_update[2], result_update[3], result_update[4]);
                }
            case "delete":
                System.out.println("Delete");
                String[] result_delete = parseDeleteTable(userCommand);
                if(result_delete.length == 1){
                    System.out.println(result_delete[0]);
                }
                else {
                    System.out.println(result_delete[0]+"\n"+ result_delete[1]+"\n"+ result_delete[2]);
                    database.deleteRowsFromTable(result_delete[0], result_delete[1], result_delete[2]);
                }
                break;
            case "help":
                Statements.help();
                break;
            case "version":
                Statements.version();
                break;
            case "exit":
            case "quit":
                database.exit();
                Settings.setExit(true);
                break;
            default:
                Statements.errorCommand(userCommand);
                break;
        }
    }
    
    public static String[] parseCreateTable(String command) {

		// TODO: Before attempting to create new table file, check if the table already exists
		
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        int i =0;
        boolean error = false;
        String[] error_str = new String[4];

        System.out.println(createTableTokens);
        if (!createTableTokens.get(1).equals("table")) {
            error_str[0] = Statements.SYNTAX_ERROR;
            error = true;
        }
        String tableName = createTableTokens.get(2);
        
        if (tableName.trim().length() == 0 && !error) {
            error_str[0] = Statements.TABLE_NAME_EMPTY;
            error = true;
        }
        if(!error) {
            String column_names_tuple = createTableTokens.get(3);
            String column_names = column_names_tuple.replaceAll("[()]", " ");
            String[] values = new String[4];

            values[0] = tableName;
            values[1] = column_names;

            return values;
            
        }
        else {
            return error_str;
        }
	}

    public static String[] parseDeleteTable(String command) {
        ArrayList<String> deleteTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        String[] error_str = new String[4];
        String tableName = "";
        String[] condition = command.split("where");
        String[] values = condition[1].split("=");

        if (!deleteTableTokens.get(1).equals("from") || !deleteTableTokens.get(2).equals("table") || !deleteTableTokens.get(4).equals("where")) {
            System.out.println(Statements.SYNTAX_ERROR);
            error_str[0] = Statements.SYNTAX_ERROR;
            return error_str;
        }
        tableName = deleteTableTokens.get(3);
        String col_name = values[0];
        String col_value = values[1];
        String[] result_str = new String[4];

        result_str[0] = tableName;
        result_str[1] = col_name;
        result_str[2] = col_value;

        return result_str;
    }

    public static String[] parseInsertTable(String command) {
        ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        String[] result = new String[2];
        if (!insertTokens.get(1).equals("into") || !command.contains(") values")) {
            System.out.println(Statements.SYNTAX_ERROR);
            result[0] = Statements.SYNTAX_ERROR;
            return result;
        }
        String tableName = insertTokens.get(2);
        if (tableName.trim().length() == 0) {
            System.out.println(Statements.TABLE_NAME_EMPTY);
            result[0] = Statements.TABLE_NAME_EMPTY;
            return result;
        }
        else {
            String value_list = insertTokens.get(5);
            String values = value_list.replaceAll("[()]", "");
            result[0] = tableName;
            result[1] = values;

            return result;
        }
    }

    public static String parseDropTable(String command) {

        String[] tokens = command.split(" ");
        if (!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
            System.out.println(Statements.SYNTAX_ERROR);
            return "";
        }

        ArrayList<String> dropTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        String tableName = dropTableTokens.get(2);
        System.out.println(tableName);
        return tableName;
    }

    public static String[] parseSelectQuery(String command) {
        String table_name = "";
        List<String> column_names = new ArrayList<String>();
        String condition_variable = "";
        String condition_value = "";
        String[] error_str = new String[4];

        // Get table and column names for the select
        ArrayList<String> queryTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        int i = 0;
        boolean checked=false;
        for (i = 1; i < queryTableTokens.size(); i++) {
            // System.out.println(queryTableTokens.get(i));
            if (queryTableTokens.get(i).equals("from")) {
                ++i;
                table_name = queryTableTokens.get(i);
                checked = true;
                continue;
            }
            else if (queryTableTokens.get(i).equals("where")) {
                ++i;
                condition_variable = queryTableTokens.get(i);
                condition_value = queryTableTokens.get(i+2);
                // break;
            }
            if (!queryTableTokens.get(i).equals("*") && !queryTableTokens.get(i).equals(",") && !checked) {
                if (queryTableTokens.get(i).contains(",")) {
                    ArrayList<String> colList = new ArrayList<String>(
                            Arrays.asList(queryTableTokens.get(i).split(",")));
                    for (String col : colList) {
                        column_names.add(col.trim());
                    }
                } else
                    column_names.add(queryTableTokens.get(i));
            }
            else {
                checked = true;

            }
            
        }
        String delim = ",";
        String res = "";
        if(column_names.size() == 0) {
            res="";
        }
        else {
            StringBuilder sb = new StringBuilder();
 
            for(String str : column_names)
            {
                // System.out.println(str);
                sb.append(str);
                //for adding comma between elements
                sb.append(delim);
            }
            //just for removing last comma
            sb.setLength(sb.length()-1);
            res = sb.toString();
        }
        String values[] = new String[4];
        values[0] = table_name;
        values[1] = res;
        values[2] = condition_variable;
        values[3] = condition_value;
        

        return values;
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

    public static String[] parseUpdateTable(String command) {
        String[] tokens = command.split(" ");
        String[] condition = command.split("where");
        String[] values = condition[1].split("=");

        String[] error_str = new String[4];
        if (!(tokens[0].trim().equalsIgnoreCase("UPDATE") && tokens[2].trim().equalsIgnoreCase("SET"))|| !tokens[6].trim().equalsIgnoreCase("where")) {
            error_str[0] = Statements.SYNTAX_ERROR;
            return error_str;
        }
        else {
            String[] result_str = new String[5];
            result_str[0] = tokens[1].trim();
            result_str[1] = tokens[3].trim();
            result_str[2] = tokens[5].trim();
            result_str[3] = values[0].trim();
            result_str[4] = values[1].trim();
            return result_str;
        }
    }

}
