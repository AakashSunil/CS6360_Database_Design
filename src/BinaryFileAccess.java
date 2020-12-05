import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinaryFileAccess {
    public static String columns_Table = "DavisBase_Columns";
    public static String tables_Table = "DavisBase_Tables";
    public static boolean rowId = false;
    public static boolean dataStoreInitialized = false;

    static int pageSizePower = 9;
    public static int pageSize = (int) Math.pow(2, pageSizePower);

    RandomAccessFile file;

    public BinaryFileAccess(RandomAccessFile file) {
        this.file = file;
    }

    public boolean recordExists(MetaData tablemetaData, List<String> columNames, ConditionalDetails condition) throws IOException {

        BPlusTree bPlusTree = new BPlusTree(file, tablemetaData.root_page_no, tablemetaData.table_name);


        for (Integer pageNo : bPlusTree.getLeaves(condition)) {
            PageTrack page = new PageTrack(file, pageNo);
            for (TableRecords record : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.check(record.getFields().get(condition.column_Ordinal).fieldValue))
                        continue;
                }
                return true;
            }
        }
        return false;

    }


    public int updateRecords(MetaData tablemetaData, ConditionalDetails condition,
                             List<String> columNames, List<String> newValues) throws IOException {
        int count = 0;


        List<Integer> ordinalPostions = tablemetaData.getOrdinalPostions(columNames);

        int k = 0;
        Map<Integer, FieldFormat> newValueMap = new HashMap<>();

        for (String strnewValue : newValues) {
            int index = ordinalPostions.get(k);

            try {
                newValueMap.put(index,
                        new FieldFormat(tablemetaData.column_name_attrs.get(index).datatype, strnewValue));
            } catch (Exception e) {
                System.out.println("Invalid data format for " + tablemetaData.column_names.get(index) + " values: "
                        + strnewValue);
                return count;
            }

            k++;
        }

        BPlusTree bPlusTree = new BPlusTree(file, tablemetaData.root_page_no, tablemetaData.table_name);

        for (Integer pageNo : bPlusTree.getLeaves(condition)) {
            short deleteCountPerPage = 0;
            PageTrack page = new PageTrack(file, pageNo);
            for (TableRecords record : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.check(record.getFields().get(condition.column_Ordinal).fieldValue))
                        continue;
                }
                count++;
                for (int i : newValueMap.keySet()) {
                    FieldFormat oldValue = record.getFields().get(i);
                    int rowId = record.row_ID;
                    if ((record.getFields().get(i).dataType == DataFormat.TEXT
                            && record.getFields().get(i).fieldValue.length() == newValueMap.get(i).fieldValue.length())
                            || (record.getFields().get(i).dataType != DataFormat.NULL && record.getFields().get(i).dataType != DataFormat.TEXT)
                    ) {
                        page.updateRecord(record, i, newValueMap.get(i).fieldValueByte);
                    } else {

                        page.DeleteTableRec(tablemetaData.table_name,
                                Integer.valueOf(record.pageHeadIndex - deleteCountPerPage).shortValue());
                        deleteCountPerPage++;
                        List<FieldFormat> attrs = record.getFields();
                        FieldFormat attr = attrs.get(i);
                        attrs.remove(i);
                        attr = newValueMap.get(i);
                        attrs.add(i, attr);
                        rowId = page.addTableRow(tablemetaData.table_name, attrs);
                    }

                    if (tablemetaData.column_name_attrs.get(i).hasIndex && condition != null) {
                        RandomAccessFile indexFile = new RandomAccessFile(FileFunctions.getNDXFilePath(tablemetaData.column_name_attrs.get(i).table_name, tablemetaData.column_name_attrs.get(i).column_Name), "rw");
                        Page bTree = new Page(indexFile);
                        bTree.delete(oldValue, record.row_ID);
                        bTree.insert(newValueMap.get(i), rowId);
                        indexFile.close();
                    }

                }
            }
        }

        if (!tablemetaData.table_name.equals(tables_Table) && !tablemetaData.table_name.equals(columns_Table))
            System.out.println("* " + count + " record(s) updated.");

        return count;

    }

    public void selectRecords(MetaData tablemetaData, List<String> columNames, ConditionalDetails condition) throws IOException {

        List<Integer> ordinalPostions = tablemetaData.getOrdinalPostions(columNames);

        System.out.println();

        List<Integer> printPosition = new ArrayList<>();

        int columnPrintLength = 0;
        printPosition.add(columnPrintLength);
        int totalTablePrintLength = 0;
        if (rowId) {
            System.out.print("rowid");
            System.out.print(FileFunctions.line(" ", 5));
            printPosition.add(10);
            totalTablePrintLength += 10;
        }


        for (int i : ordinalPostions) {
            String columnName = tablemetaData.column_name_attrs.get(i).column_Name;
            columnPrintLength = Math.max(columnName.length()
                    , tablemetaData.column_name_attrs.get(i).datatype.getPrintOffset()) + 5;
            printPosition.add(columnPrintLength);
            System.out.print(columnName);
            System.out.print(FileFunctions.line(" ", columnPrintLength - columnName.length()));
            totalTablePrintLength += columnPrintLength;
        }
        System.out.println();
        System.out.println(FileFunctions.line("*", totalTablePrintLength));

        BPlusTree bPlusTree = new BPlusTree(file, tablemetaData.root_page_no, tablemetaData.table_name);

        String currentValue = "";
        for (Integer pageNo : bPlusTree.getLeaves(condition)) {
            PageTrack page = new PageTrack(file, pageNo);
            for (TableRecords record : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.check(record.getFields().get(condition.column_Ordinal).fieldValue))
                        continue;
                }
                int columnCount = 0;
                if (rowId) {
                    currentValue = Integer.valueOf(record.row_ID).toString();
                    System.out.print(currentValue);
                    System.out.print(FileFunctions.line(" ", printPosition.get(++columnCount) - currentValue.length()));
                }
                for (int i : ordinalPostions) {
                    currentValue = record.getFields().get(i).fieldValue;
                    System.out.print(currentValue);
                    System.out.print(FileFunctions.line(" ", printPosition.get(++columnCount) - currentValue.length()));
                }
                System.out.println();
            }
        }

        System.out.println();

    }


    public static int getRootPageNo(RandomAccessFile binaryfile) {
        int rootpage = 0;
        try {
            for (int i = 0; i < binaryfile.length() / BinaryFileAccess.pageSize; i++) {
                binaryfile.seek(i * BinaryFileAccess.pageSize + 0x0A);
                int a = binaryfile.readInt();

                if (a == -1) {
                    return i;
                }
            }
            return rootpage;
        } catch (Exception e) {
            System.out.println("error while getting root page no ");
            System.out.println(e);
        }
        return -1;

    }

    public static void initializeDataStore() {

        try {
            File dataDir = new File("data");
            dataDir.mkdir();
            String[] oldTableFiles;
            oldTableFiles = dataDir.list();
            for (int i = 0; i < oldTableFiles.length; i++) {
                File anOldFile = new File(dataDir, oldTableFiles[i]);
                anOldFile.delete();
            }
        } catch (SecurityException se) {
            System.out.println("Unable to create data container directory");
            System.out.println(se);
        }

        try {

            int currentPageNo = 0;

            RandomAccessFile DavisBaseTablesCatalog = new RandomAccessFile(
                    FileFunctions.getTBLFile(tables_Table), "rw");
            PageTrack.addNewPage(DavisBaseTablesCatalog, PageType.LEAF, -1, -1);
            PageTrack page = new PageTrack(DavisBaseTablesCatalog, currentPageNo);

            page.addTableRow(tables_Table, Arrays.asList(new FieldFormat(DataFormat.TEXT, BinaryFileAccess.tables_Table),
                    new FieldFormat(DataFormat.INT, "2"),
                    new FieldFormat(DataFormat.SMALLINT, "0"),
                    new FieldFormat(DataFormat.SMALLINT, "0")));

            page.addTableRow(tables_Table, Arrays.asList(new FieldFormat(DataFormat.TEXT, BinaryFileAccess.columns_Table),
                    new FieldFormat(DataFormat.INT, "11"),
                    new FieldFormat(DataFormat.SMALLINT, "0"),
                    new FieldFormat(DataFormat.SMALLINT, "2")));

            DavisBaseTablesCatalog.close();
        } catch (Exception e) {
            System.out.println("Unable to create the database_tables file");
            System.out.println(e);


        }

        try {
            RandomAccessFile DavisBaseColumnsCatalog = new RandomAccessFile(
                    FileFunctions.getTBLFile(columns_Table), "rw");
            PageTrack.addNewPage(DavisBaseColumnsCatalog, PageType.LEAF, -1, -1);
            PageTrack page = new PageTrack(DavisBaseColumnsCatalog, 0);

            short ordinal_position = 1;

            page.addNewColumn(new ColumnDetails(tables_Table, DataFormat.TEXT, "table_name", true, false, ordinal_position++));
            page.addNewColumn(new ColumnDetails(tables_Table, DataFormat.INT, "record_count", false, false, ordinal_position++));
            page.addNewColumn(new ColumnDetails(tables_Table, DataFormat.SMALLINT, "avg_length", false, false, ordinal_position++));
            page.addNewColumn(new ColumnDetails(tables_Table, DataFormat.SMALLINT, "root_page", false, false, ordinal_position++));

            ordinal_position = 1;

            page.addNewColumn(new ColumnDetails(columns_Table, DataFormat.TEXT, "table_name", false, false, ordinal_position++));
            page.addNewColumn(new ColumnDetails(columns_Table, DataFormat.TEXT, "column_name", false, false, ordinal_position++));
            page.addNewColumn(new ColumnDetails(columns_Table, DataFormat.SMALLINT, "data_type", false, false, ordinal_position++));
            page.addNewColumn(new ColumnDetails(columns_Table, DataFormat.SMALLINT, "ordinal_position", false, false, ordinal_position++));
            page.addNewColumn(new ColumnDetails(columns_Table, DataFormat.TEXT, "is_nullable", false, false, ordinal_position++));
            page.addNewColumn(new ColumnDetails(columns_Table, DataFormat.SMALLINT, "column_key", false, true, ordinal_position++));
            page.addNewColumn(new ColumnDetails(columns_Table, DataFormat.SMALLINT, "is_unique", false, false, ordinal_position++));

            DavisBaseColumnsCatalog.close();
            dataStoreInitialized = true;
        } catch (Exception e) {
            System.out.println("Unable to create the database_columns file");
            System.out.println(e);
        }
    }
}
