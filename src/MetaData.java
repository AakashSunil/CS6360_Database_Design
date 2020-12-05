

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MetaData {

    public int record_count;
    public List<TableRecords> columnData;
    public List<ColumnDetails> column_name_attrs;
    public List<String> column_names;
    public String table_name;
    public boolean tableExists;
    public int root_page_no;
    public int lastRowId;

    public MetaData(String table_name) {
        this.table_name = table_name;
        tableExists = false;
        try {

            RandomAccessFile DavisBaseTablesCatalog = new RandomAccessFile(
                    FileFunctions.getTBLFile(BinaryFileAccess.tables_Table), "r");

            int root_page_no = BinaryFileAccess.getRootPageNo(DavisBaseTablesCatalog);

            BPlusTree bplusTree = new BPlusTree(DavisBaseTablesCatalog, root_page_no, table_name);

            for (Integer pageNo : bplusTree.getLeaves()) {
                PageTrack page = new PageTrack(DavisBaseTablesCatalog, pageNo);

                for (TableRecords record : page.getPageRecords()) {
                    if (record.getFields().get(0).fieldValue.equals(table_name)) {
                        this.root_page_no = Integer.parseInt(record.getFields().get(3).fieldValue);
                        record_count = Integer.parseInt(record.getFields().get(1).fieldValue);
                        tableExists = true;
                        break;
                    }
                }
                if (tableExists)
                    break;
            }

            DavisBaseTablesCatalog.close();
            if (tableExists) {
                loadColumnData();
            } else {
                throw new Exception("Table does not exist.");
            }

        } catch (Exception e) {
        }
    }

    public List<Integer> getOrdinalPostions(List<String> columns) {
        List<Integer> ordinalPostions = new ArrayList<>();
        for (String column : columns) {
            ordinalPostions.add(column_names.indexOf(column));
        }
        return ordinalPostions;
    }

    private void loadColumnData() {
        try {

            RandomAccessFile DavisBaseColumnsCatalog = new RandomAccessFile(
                    FileFunctions.getTBLFile(BinaryFileAccess.columns_Table), "r");
            int root_page_no = BinaryFileAccess.getRootPageNo(DavisBaseColumnsCatalog);

            columnData = new ArrayList<>();
            column_name_attrs = new ArrayList<>();
            column_names = new ArrayList<>();
            BPlusTree bPlusTree = new BPlusTree(DavisBaseColumnsCatalog, root_page_no, table_name);

            for (Integer pageNo : bPlusTree.getLeaves()) {

                PageTrack page = new PageTrack(DavisBaseColumnsCatalog, pageNo);

                for (TableRecords record : page.getPageRecords()) {

                    if (record.getFields().get(0).fieldValue.equals(table_name)) {
                        {
                            columnData.add(record);
                            column_names.add(record.getFields().get(1).fieldValue);
                            ColumnDetails colInfo = new ColumnDetails(
                                    table_name
                                    , DataFormat.get(record.getFields().get(2).fieldValue)
                                    , record.getFields().get(1).fieldValue
                                    , record.getFields().get(6).fieldValue.equals("YES")
                                    , record.getFields().get(4).fieldValue.equals("YES")
                                    , Short.parseShort(record.getFields().get(3).fieldValue)
                            );

                            if (record.getFields().get(5).fieldValue.equals("PRI"))
                                colInfo.setAsPrimaryKey();

                            column_name_attrs.add(colInfo);


                        }
                    }
                }
            }

            DavisBaseColumnsCatalog.close();
        } catch (Exception e) {
            System.out.println("Error while getting column data for " + table_name);
        }

    }

    public boolean columnExists(List<String> columns) {

        if (columns.size() == 0)
            return true;

        List<String> lColumns = new ArrayList<>(columns);

        for (ColumnDetails column_name_attr : column_name_attrs) {
            lColumns.remove(column_name_attr.column_Name);
        }

        return lColumns.isEmpty();
    }


    public void updateMetaData() {

        try {
            RandomAccessFile tableFile = new RandomAccessFile(
                    FileFunctions.getTBLFile(table_name), "r");

            Integer root_page_no = BinaryFileAccess.getRootPageNo(tableFile);
            tableFile.close();


            RandomAccessFile DavisBaseTablesCatalog = new RandomAccessFile(
                    FileFunctions.getTBLFile(BinaryFileAccess.tables_Table), "rw");

            BinaryFileAccess tablesBinaryFile = new BinaryFileAccess(DavisBaseTablesCatalog);

            MetaData tablesMetaData = new MetaData(BinaryFileAccess.tables_Table);

            ConditionalDetails condition = new ConditionalDetails(DataFormat.TEXT);
            condition.setColumName("table_name");
            condition.column_Ordinal = 0;
            condition.setConditionValue(table_name);
            condition.setOperator("=");

            List<String> columns = Arrays.asList("record_count", "root_page");
            List<String> newValues = new ArrayList<>();

            newValues.add(Integer.toString(record_count));
            newValues.add(Integer.toString(root_page_no));

            tablesBinaryFile.updateRecords(tablesMetaData, condition, columns, newValues);

            DavisBaseTablesCatalog.close();
        } catch (IOException e) {
            System.out.println("Error updating meta data for " + table_name);
        }


    }

    public boolean validateInsert(List<FieldFormat> row) throws IOException {
        RandomAccessFile tableFile = new RandomAccessFile(FileFunctions.getTBLFile(table_name), "r");
        BinaryFileAccess file = new BinaryFileAccess(tableFile);


        for (int i = 0; i < column_name_attrs.size(); i++) {

            ConditionalDetails condition = new ConditionalDetails(column_name_attrs.get(i).datatype);
            condition.columnName = column_name_attrs.get(i).column_Name;
            condition.column_Ordinal = i;
            condition.setOperator("=");

            if (column_name_attrs.get(i).isUnique) {
                condition.setConditionValue(row.get(i).fieldValue);
                if (file.recordExists(this, Arrays.asList(column_name_attrs.get(i).column_Name), condition)) {
                    System.out.println("Insert failed: ColumnDetails " + column_name_attrs.get(i).column_Name + " should be unique.");
                    tableFile.close();
                    return false;
                }


            }


        }
        tableFile.close();
        return true;
    }
    
}
