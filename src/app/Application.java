package app;
import java.util.Scanner;

import utils.Commands;
import utils.Settings;
import utils.Statements;

public class Application {
    
    public static void main(String args[]) {
        

       Statements.splashScreen();
       String userCommand = "";
       Scanner scanner = new Scanner(System.in).useDelimiter(";");
       Commands commands = new Commands();

       while (!Settings.isExit()) {
           System.out.print(Settings.getPrompt());
           userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
           commands.parseUserCommand(userCommand);
       }
       System.out.println("Exiting.....");
       System.out.println(Statements.EXIT);
       scanner.close();
    }

    
}