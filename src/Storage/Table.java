package Storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class Table implements Serializable {
    ArrayList<String> columnNameList;
    ArrayList<ArrayList<String>> rows;

    public Table(ArrayList<String> columnNameList) {
        this.columnNameList = columnNameList;
        this.rows = new ArrayList<>();
    }

    public void insert(ArrayList<String> valueList) {
        this.rows.add(valueList);
    }

    public void delete(String columnName, String value) {
        int index = columnNameList.indexOf(columnName);
        rows = (ArrayList<ArrayList<String>>)
                rows
                .stream()
                .filter(row -> (row.get(index) != value))
                .collect(Collectors.toList());
    }

    public void update(String setColumnName, String setValue,
                       String condColumnName, String condValue) {
        int setIndex = columnNameList.indexOf(setColumnName);
        int condIndex = columnNameList.indexOf(condColumnName);
        for (ArrayList<String> row : rows) {
            if (row.get(condIndex) == condValue) {
                row.set(setIndex, setValue);
            }
        }
    }

    public ArrayList<ArrayList<String>> query(ArrayList<String> queryColumnNameList,
                                              String condColumnName, String condValue) {
        ArrayList<ArrayList<String>> queryResultList = new ArrayList<>();
        ArrayList<Integer> queryIndexList = new ArrayList<>();
        for (String columnName : queryColumnNameList) {
            queryIndexList.add(columnNameList.indexOf(columnName));
        }
        int condIndex = columnNameList.indexOf(condColumnName);

        for (ArrayList<String> row : rows) {
            if (row.get(condIndex) == condValue) {
                ArrayList<String> result = new ArrayList<>();
                for (int index : queryIndexList) {
                    result.add(row.get(index));
                }
                queryResultList.add(result);
            }
        }

        return queryResultList;
    }
}
