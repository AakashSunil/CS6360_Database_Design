

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Commands {
    public static void parseUserCommand(String userCommand) {

        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
        switch (commandTokens.get(0)) {
            case "show":
                if(commandTokens.get(1).equals("tables")) {
                    parseUserCommand("select * from DavisBase_Tables");
                }
                else if(commandTokens.get(1).equals("row_id")) {
                    BinaryFileAccess.rowId = true;
                    System.out.println(Statements.ROW_ID);
                }
                else {
                    System.out.println(Statements.INVALID_COMMAND + userCommand);
                }
                break;
            case "select":
                parseSelectQuery(userCommand);
                break;
            case "drop":
                parseDropTable(userCommand);
                break;
            case "create":
                if (commandTokens.get(1).equals("table"))
                    parseCreateTable(userCommand);
                else if (commandTokens.get(1).equals("index"))
                    parseCreateIndex(userCommand);
                break;
            case "insert":
                parseInsertTable(userCommand);
                break;
            case "update":
                parseUpdateTable(userCommand);
                break;
            case "delete":
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

		
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        System.out.println(createTableTokens);
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
            if (tableName.indexOf("(") > -1) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }

            List<ColumnDetails> column_info = new ArrayList<>();
            ArrayList<String> col_tokens = new ArrayList<String>(Arrays.asList(command.substring(command.indexOf("(") + 1, command.length() - 1).split(",")));

            short ordinalPosition = 1;

            String primaryKeyColumn = "";

            for(String col_token: col_tokens) {

                ArrayList<String> col_info_token = new ArrayList<String>(Arrays.asList(col_token.trim().split(" ")));
                ColumnDetails col_info = new ColumnDetails();

                col_info.table_name = tableName;
                col_info.column_Name = col_info_token.get(0);
                col_info.isNullable = true;
                col_info.datatype = DataFormat.get(col_info_token.get(1).toUpperCase());
                for(int i=0;i<col_info_token.size();i++) {
                    if((col_info_token.get(i).equals("null"))) {
                        col_info.isNullable = true;
                    }
                    if (col_info_token.get(i).contains("not") && (col_info_token.get(i+1).contains("null"))) {
                        col_info.isNullable = false;
                        i++;
                    }

                    if((col_info_token.get(i).equals("unique"))) {
                        col_info.isPrimaryKey = true;
                        col_info.isUnique = true;
                        col_info.isNullable = false;
                        primaryKeyColumn = col_info.column_Name;
                        i++;
                    }
                }
                col_info.ordinalPosition = ordinalPosition++;
                column_info.add(col_info);
            }
            
            RandomAccessFile DavisBaseTable = new RandomAccessFile(FileFunctions.getTBLFile(BinaryFileAccess.tables_Table), "rw");
            MetaData DavisMetaData = new MetaData(BinaryFileAccess.tables_Table);

            int page_no = BPlusTree.getPageForInsert(DavisBaseTable,DavisMetaData.root_page_no);

            PageTrack page = new PageTrack(DavisBaseTable,page_no);

            int rowNo = page.addTableRow(BinaryFileAccess.tables_Table, Arrays.asList(new FieldFormat(DataFormat.TEXT, tableName),
            new FieldFormat(DataFormat.INT, "0"),new FieldFormat(DataFormat.SMALLINT, "0"),new FieldFormat(DataFormat.SMALLINT, "0")));
            DavisBaseTable.close();

            RandomAccessFile tbl_file = new RandomAccessFile(FileFunctions.getTBLFile(tableName), "rw");
            PageTrack.addNewPage(tbl_file,PageType.LEAF, -1,-1);
            tbl_file.close();


            RandomAccessFile DavisBaseColumn = new RandomAccessFile(FileFunctions.getTBLFile(BinaryFileAccess.columns_Table), "rw");
            MetaData DavisMData = new MetaData(BinaryFileAccess.columns_Table);
            page_no = BPlusTree.getPageForInsert(DavisBaseColumn,DavisMData.root_page_no);

            PageTrack page_1 = new PageTrack(DavisBaseColumn,page_no);

            for (ColumnDetails column : column_info) {
                page_1.addNewColumn(column);
            }

            DavisBaseColumn.close();
            System.out.println("Table Creation Successful");

            if (primaryKeyColumn.length() > 0) {
                parseCreateIndex("create index on " + tableName + "(" + primaryKeyColumn + ")");
            }
        }
        catch(Exception e) {
            System.out.println(Statements.TABLE_CREATE_ERROR);
            parseDeleteTable("delete from table "+BinaryFileAccess.tables_Table + " where table_name = '" + tableName +"' ");
            parseDeleteTable("delete from table "+BinaryFileAccess.columns_Table + " where table_name = '" + tableName +"' ");
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
            
            MetaData deleteMeta = new MetaData(tableName);
            ConditionalDetails condition = null;

            try {
                condition = FileFunctions.extractCondition(deleteMeta,command);
            }
            catch(Exception e) {
                System.out.println(e);
                return;
            }

            RandomAccessFile tbl_file = new RandomAccessFile(FileFunctions.getTBLFile(tableName), "rw");

            BPlusTree trees = new BPlusTree(tbl_file,deleteMeta.root_page_no, deleteMeta.table_name);

            List<TableRecords> deleteRecord = new ArrayList<>();
            int count = 0;
            for(int page_no : trees.getLeaves(condition)) {
                short deleteCount = 0;
                PageTrack page = new PageTrack(tbl_file,page_no);
                for(TableRecords rec : page.getPageRecords()) {
                    if(condition!=null) {
                        if(!condition.check(rec.getFields().get(condition.column_Ordinal).fieldValue))
                            continue;
                    }

                    deleteRecord.add(rec);
                    page.DeleteTableRec(tableName,Integer.valueOf(rec.pageHeadIndex - deleteCount).shortValue());
                    deleteCount++;
                    count++;
                }
            }

            if(condition == null) {

            }
            else {
                for(int i=0;i<deleteMeta.column_name_attrs.size();i++) {
                    if(deleteMeta.column_name_attrs.get(i).hasIndex) {
                        RandomAccessFile index_file = new RandomAccessFile(FileFunctions.getNDXFilePath(tableName, deleteMeta.column_name_attrs.get(i).column_Name),"rw");
                        Page tree = new Page(index_file);
                        for(TableRecords recd : deleteRecord) {
                            tree.delete(recd.getFields().get(i),recd.row_ID);
                        }
                    }
                }
            }
            tbl_file.close();
            System.out.println("Records Deleted");
        }

        catch(Exception e) {
            System.out.println("Error deleting Rows");
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

    public static void parseInsertTable(String command) {
        
        ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));

        if (!insertTokens.get(1).equals("into") || !command.contains(") values")) {
            System.out.println(Statements.SYNTAX_ERROR);
            return;
        }
        try {

            String table_Name = insertTokens.get(2);
            if (table_Name.trim().length() == 0) {
                System.out.println(Statements.TABLE_NAME_EMPTY);
                return;
            }
            if(table_Name.indexOf("(") > -1) {
                table_Name = table_Name.substring(0, table_Name.indexOf("("));
            }

            MetaData insertMData = new MetaData(table_Name);

            if(!insertMData.tableExists) {
                System.out.println(Statements.TABLE_DOES_NOT_EXIST);
                return;
            }

            ArrayList<String> column_tokens = new ArrayList<String>(Arrays.asList(command.substring(command.indexOf("(") + 1, command.indexOf(") values")).split(",")));

            for(String column: column_tokens) {
                if(!insertMData.column_names.contains(column.trim())) {
                    System.out.println(Statements.COLUMN_NAME_INVALID);
                    return;
                }
            }

            String valuesString = command.substring(command.indexOf("values") + 6, command.length() - 1);

            ArrayList<String> valueTokens = new ArrayList<String>(Arrays.asList(valuesString.substring(valuesString.indexOf("(") + 1).split(",")));

            List<FieldFormat> fieldToInsert = new ArrayList<>();

            for(ColumnDetails col: insertMData.column_name_attrs) {
                int i = 0;
                boolean col_provided = false;
                for(i=0;i<column_tokens.size();i++) {
                    if(column_tokens.get(i).trim().equals(col.column_Name)) {
                        col_provided = true;
                    
                        try {
                            String value = valueTokens.get(i).replace("'", "").replace("\"", "").trim();
                            if (valueTokens.get(i).trim().equals("null")) {
                                if(!col.isNullable) {
                                    System.out.println("Cannot Insert NULL into " + col.column_Name);
                                        return;
                                }
                                col.datatype = DataFormat.NULL;
                                value = value.toUpperCase();
                            }
                            FieldFormat attr = new FieldFormat(col.datatype, value);
                            fieldToInsert.add(attr);
                            break;
                        }
                        catch(Exception e) {
                            System.out.println("Invalid Data Format");
                            return;
                        }
                    }
                }
                if(column_tokens.size() > i) {
                    column_tokens.remove(i);
                    valueTokens.remove(i);
                }
                if(!col_provided) {
                    if(col.isNullable) {
                        fieldToInsert.add(new FieldFormat(DataFormat.NULL,"NULL"));
                    }
                    else {
                        System.out.println("Cannot insert Null");
                        return;
                    }

                }
            }

            RandomAccessFile instTable = new RandomAccessFile(FileFunctions.getTBLFile(table_Name), "rw");
            int instPageNo = BPlusTree.getPageForInsert(instTable, insertMData.root_page_no);
            PageTrack instPage = new PageTrack(instTable, instPageNo);

            int rowNo = instPage.addTableRow(table_Name, fieldToInsert);

            // update Index
            if (rowNo != -1) {

                for (int i = 0; i < insertMData.column_name_attrs.size(); i++) {
                    ColumnDetails cols = insertMData.column_name_attrs.get(i);

                    if (cols.hasIndex) {
                        RandomAccessFile indexFile = new RandomAccessFile(FileFunctions.getNDXFilePath(table_Name, cols.column_Name),
                                "rw");
                        Page bTree = new Page(indexFile);
                        bTree.insert(fieldToInsert.get(i), rowNo);
                    }

                }
            }

            instTable.close();
            if (rowNo != -1)
                System.out.println("Record Inserted");

        }
        catch (Exception ex) {
            System.out.println(Statements.INSERT_ERROR);
            System.out.println(ex);

        }
    }
  
    public static void parseSelectQuery(String command) {
        String table_name = "";
        List<String> column_names = new ArrayList<String>();

        // Get table and column names for the select
        ArrayList<String> query_table_tokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        int i = 0;
        for (i = 1; i < query_table_tokens.size(); i++) {
            // System.out.println(query_table_tokens.get(i));
            if (query_table_tokens.get(i).equals("from")) {
                ++i;
                table_name = query_table_tokens.get(i);
                break;
            }
            if (!query_table_tokens.get(i).equals("*") && !query_table_tokens.get(i).equals(",")) {
                if (query_table_tokens.get(i).contains(",")) {
                    ArrayList<String> colList = new ArrayList<String>(
                            Arrays.asList(query_table_tokens.get(i).split(",")));
                    for (String col : colList) {
                        column_names.add(col.trim());
                    }
                } else
                    column_names.add(query_table_tokens.get(i));
            }
        }

        MetaData table_mData = new MetaData(table_name);
        if(!table_mData.tableExists) {
            System.out.println(Statements.TABLE_DOES_NOT_EXIST);
            return;
        }

        ConditionalDetails condition = null;

        try {
            condition = FileFunctions.extractCondition(table_mData,command);
        }
        catch(Exception e) {
            System.out.println(e.getMessage());
            return;
        }


        if(column_names.size() == 0) {
            column_names = table_mData.column_names;
        }

        try {
            RandomAccessFile tbl_file = new RandomAccessFile(FileFunctions.getTBLFile(table_name), "r");
            BinaryFileAccess tbl_binary_file = new BinaryFileAccess(tbl_file);
            tbl_binary_file.selectRecords(table_mData,column_names,condition);
            tbl_file.close();
        }
        catch (IOException exception) {
            System.out.println(Statements.ERROR_SELECT_COLUMN);
        }
    }

    public static void parseDropTable(String command) {

        String[] tokens = command.split(" ");
        if (!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
            System.out.println(Statements.SYNTAX_ERROR);
            return;
        }

        ArrayList<String> dropTableTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        String table_Name = dropTableTokens.get(2);

        parseDeleteTable("delete from table " + BinaryFileAccess.tables_Table + " where table_name = '" + table_Name + "' ");
        parseDeleteTable("delete from table " + BinaryFileAccess.columns_Table + " where table_name = '" + table_Name + "' ");

        File table_file = new File("data/"+table_Name+".tbl");

        if(table_file.delete()) {
            System.out.println(Statements.TABLE_DELETED);
        }
        else {
            System.out.println(Statements.TABLE_DOES_NOT_EXIST);
        }

        File files = new File("data/");
        File[] file_match = files.listFiles(new FilenameFilter() {
            public boolean accept(File directory, String name) {
                return name.startsWith(table_Name) && name.endsWith("ndx");
            }
        });

        boolean iFlag = false;
        for(File file_value : file_match) {
            if(file_value.delete()) {
                iFlag = true;
                System.out.println(Statements.INDEX_DELETED);
            }
        }
        if(iFlag) {
            System.out.println("Drop "+ table_Name);
        }
        else {
            System.out.println(Statements.INDEX_DOES_NOT_EXIST);
        }
    }

    public static void parseCreateIndex(String command) {
        ArrayList<String> createIndexTokens = new ArrayList<String>(Arrays.asList(command.split(" ")));
        try {
            if (!createIndexTokens.get(2).equals("on") || !command.contains("(")
                    || !command.contains(")") && createIndexTokens.size() < 4) {
                System.out.println(Statements.SYNTAX_ERROR);
                return;
            }

            String table_Name = command
                    .substring(command.indexOf("on") + 3, command.indexOf("(")).trim();
            String column_Name = command
                    .substring(command.indexOf("(") + 1, command.indexOf(")")).trim();

            if(new File(FileFunctions.getNDXFilePath(table_Name, column_Name)).exists()) {
                System.out.println(Statements.EXISTING_INDEX);
                return;
            }

            RandomAccessFile tbl_File = new RandomAccessFile(FileFunctions.getTBLFile(table_Name), "rw");
            MetaData mData = new MetaData(table_Name);

            if(!mData.tableExists) {
                System.out.println(Statements.TABLE_NAME_INVALID);
                tbl_File.close();
                return;
            }

            int ordinalColumn = mData.column_names.indexOf(column_Name);

            if(ordinalColumn < 0) {
                System.out.println(Statements.COLUMN_NAME_INVALID);
                tbl_File.close();
                return;
            }

            RandomAccessFile index_files = new RandomAccessFile(FileFunctions.getNDXFilePath(table_Name,column_Name), "rw");
            PageTrack.addNewPage(index_files,PageType.LEAF_INDEX, -1, -1);
            
            if(mData.record_count > 0) {
                BPlusTree bplustree = new BPlusTree(tbl_File,mData.root_page_no, mData.table_name);
                for (int page_no: bplustree.getLeaves()) {
                    PageTrack page = new PageTrack(tbl_File,page_no);
                    Page btree = new Page(index_files);
                    for (TableRecords rec : page.getPageRecords()) {
                        btree.insert(rec.getFields().get(ordinalColumn),rec.row_ID);
                    }
                }
            }

            System.out.println(Statements.CREATED_INDEX + column_Name);
            index_files.close();
            tbl_File.close();

        } catch (Exception e) {

            System.out.println(Statements.INDEX_ERROR);
            System.out.println(e);
        }

    }

}