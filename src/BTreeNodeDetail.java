

import java.util.List;

public class BTreeNodeDetail {
    public FieldFormat indexValue;
    public List<Integer> rowids;
    public boolean isInteriorNode;
    public int leftPageNo;

    public BTreeNodeDetail(FieldFormat indexValue, List<Integer> rowids) {
        this.indexValue = indexValue;
        this.rowids = rowids;
    }
}
