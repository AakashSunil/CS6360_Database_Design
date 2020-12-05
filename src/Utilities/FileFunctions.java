package Utilities;

import java.util.ArrayList;
import java.util.Arrays;

import DBDetails.ConditionalDetails;
import DBDetails.DataFormat;
import DBDetails.MetaData;

public class FileFunctions {
    public static String getTBLFile(String table_name) {
        return "data/" + table_name + ".tbl";
    }

    public static String getNDXFilePath(String table_name, String columnName) {
        return "data/" + table_name + "_" + columnName + ".ndx";
    }

    public static String line(String s, int num) {
        String a = "";
        for (int i = 0; i < num; i++) {
            a += s;
        }
        return a;
    }

    public static ConditionalDetails extractCondition(MetaData tableMetaData, String query) throws Exception {
        if (query.contains("where")) {
            ConditionalDetails condition = new ConditionalDetails(DataFormat.TEXT);
            String whereClause = query.substring(query.indexOf("where") + 6);
            ArrayList<String> whereClauseTokens = new ArrayList<String>(Arrays.asList(whereClause.split(" ")));

            if (whereClauseTokens.get(0).equalsIgnoreCase("not")) {
                condition.setNegation(true);
            }


            for (int i = 0; i < ConditionalDetails.supportedOperators.length; i++) {
                if (whereClause.contains(ConditionalDetails.supportedOperators[i])) {
                    whereClauseTokens = new ArrayList<String>(
                            Arrays.asList(whereClause.split(ConditionalDetails.supportedOperators[i])));
                    {
                        condition.setOperator(ConditionalDetails.supportedOperators[i]);
                        condition.setConditionValue(whereClauseTokens.get(1).trim());
                        condition.setColumName(whereClauseTokens.get(0).trim());
                        break;
                    }

                }
            }


            if (tableMetaData.tableExists
                    && tableMetaData.columnExists(new ArrayList<String>(Arrays.asList(condition.columnName)))) {
                condition.column_Ordinal = tableMetaData.column_names.indexOf(condition.columnName);
                condition.dataType = tableMetaData.column_name_attrs.get(condition.column_Ordinal).datatype;
            } else {
                throw new Exception(
                        "Invalid Table/ColumnDetails : " + tableMetaData.table_name + " . " + condition.columnName);
            }
            return condition;
        } else
            return null;
    }
}
