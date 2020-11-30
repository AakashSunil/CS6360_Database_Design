package app;
import java.util.ArrayList;
import java.util.Arrays;

import Storage.Database;

public class Application {
    
    public static void main(String args[]) {
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

        database.exit();

//        Statements.splashScreen();
//        String userCommand = "";
//        Scanner scanner = new Scanner(System.in).useDelimiter(";");
//
//        while (!Settings.isExit()) {
//            System.out.print(Settings.getPrompt());
//            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
//            Commands.parseUserCommand(userCommand);
//        }
//        System.out.println("Exiting.....");
//        System.out.println(Statements.EXIT);
//        scanner.close();
    }

    
}