package Storage;

public class TableRecordInteriorWorking {
    public int rowId;
    public int leftChildPageNo;

    public TableRecordInteriorWorking(int rowId, int leftChildPageNo) {
        this.rowId = rowId;
        this.leftChildPageNo = leftChildPageNo;
    }
}
