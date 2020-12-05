

import java.io.File;

public class ColumnDetails {
    public DataFormat datatype;

    public String column_Name;

    public boolean isUnique;
    public boolean isNullable;
    public Short ordinalPosition;
    public boolean hasIndex;
    public String table_name;
    public boolean isPrimaryKey;

    public ColumnDetails() {

    }

    public ColumnDetails(String tableName, DataFormat dataType, String columnName, boolean isUnique, boolean isNullable, short ordinalPosition) {
        this.datatype = dataType;
        this.column_Name = columnName;
        this.isUnique = isUnique;
        this.isNullable = isNullable;
        this.ordinalPosition = ordinalPosition;
        this.table_name = tableName;

        this.hasIndex = (new File(FileFunctions.getNDXFilePath(tableName, columnName)).exists());

    }


    public void setAsPrimaryKey() {
        isPrimaryKey = true;
    }
}
