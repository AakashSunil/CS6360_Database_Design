package app;

import java.io.File;
import java.util.Scanner;

import Storage.BinaryFileAccess;
import Utilities.Commands;
import Utilities.Settings;
import Utilities.Statements;

public class Application {
    public static void main(String args[]) {
        

       Statements.splashScreen();

       File dataDir = new File("data");

        if (!new File(dataDir, BinaryFileAccess.tables_Table + ".tbl").exists()
                || !new File(dataDir, BinaryFileAccess.columns_Table + ".tbl").exists())
            BinaryFileAccess.initializeDataStore();
        else
            BinaryFileAccess.dataStoreInitialized = true;
       String userCommand = "";
       Scanner scanner = new Scanner(System.in).useDelimiter(";");

       while (!Settings.isExit()) {
           System.out.print(Settings.getPrompt());
           userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
           Commands.parseUserCommand(userCommand);
       }
       System.out.println("Exiting.....");
       System.out.println(Statements.EXIT);
       scanner.close();
    }
}