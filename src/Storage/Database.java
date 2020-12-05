package Storage;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Database implements Serializable {
    static final String FILE_PATH = "src/Storage/Data/database.tbl";
    HashMap<String, Table> tableHashMap;    // <tableName, Table>

    // load database from file or create an empty database if no file has been found
    public void initial() {
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(FILE_PATH));
            tableHashMap = (HashMap<String, Table>) ois.readObject();
        } catch (FileNotFoundException e) {
            tableHashMap = new HashMap<>();
//            tableHashMap.put("davisbase_tables")
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // dump database to file
    public void exit() {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(FILE_PATH));
            oos.writeObject(tableHashMap);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // show tables
    public ArrayList<String> showTables() {
        return new ArrayList<>(tableHashMap.keySet());
    }

    /*
    * create a table schema like
    * CREATE TABLE table_name (
    *   column_name1 data_type1 [NOT NULL][UNIQUE],
    *   column_name2 data_type2 [NOT NULL][UNIQUE],
    * ...);
    *
    * example:
    *   tableName: "Student"
    *   columnNameList: ["name", "age", "gender"]
    * */
    public void createTable(String tableName, ArrayList<String> columnNameList) {
        tableHashMap.put(tableName, new Table(columnNameList));
    }

    /*
    * example:
    *   tableName: "Student"
    * */
    public void dropTable(String tableName) {
        tableHashMap.remove(tableName);
    }

    /*
    * insert a row like
    * INSERT INTO TABLE (column_list) table_name VALUES (value1,value2,value3, ...);
    *
    * example:
    *   tableName: "Student"
    *   valueList: ["John", "24", "male"]   (should be in the corresponding order with table creating)
    * */
    public void insertRowIntoTable(String tableName, ArrayList<String> valueList) {
        tableHashMap.get(tableName).insert(valueList);
    }

    /*
     * delete rows like
     * DELETE FROM TABLE table_name [WHERE condition];
     *
     * example:
     *   tableName: "Student"
     *   columnName: "age"
     *   value: "24"    (only support equalities(=) condition now)
     * */
    public void deleteRowsFromTable(String tableName, String columnName, String value) {
        tableHashMap.get(tableName).delete(columnName, value);
    }

    /*
     * update rows like
     * UPDATE table_name SET column_name = value WHERE condition;
     *
     * example:
     *   tableName: "Student"
     *   setColumnName: "age"
     *   setValue: "25"
     *   condColumnName:  "name"
     *   condValue: "Taylor" (only support equalities(=) condition now)
     * */
    public void updateRowsInTable(String tableName, String setColumnName, String setValue,
                                  String condColumnName, String condValue) {
        tableHashMap.get(tableName).update(setColumnName, setValue, condColumnName, condValue);
    }

    /*
     * select statement like
     * SELECT *
     * FROM table_name
     * WHERE [NOT] condition;
     *
     * example:
     *   tableName: "Student"
     *   queryColumnNameList: ["gender", "name"]
     *   condColumnName:  "age"
     *   condValue: "25" (only support equalities(=) condition now)
     * */
    public ArrayList<ArrayList<String>> queryInTable(String tableName, ArrayList<String> queryColumnNameList,
                             String condColumnName, String condValue) {
        // System.out.println(tableHashMap.get(tableName).query(queryColumnNameList, condColumnName, condValue));
        return tableHashMap.get(tableName).query(queryColumnNameList, condColumnName, condValue);
    }

}
