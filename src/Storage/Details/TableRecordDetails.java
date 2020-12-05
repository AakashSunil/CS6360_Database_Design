package Storage.Details;

import java.util.List;

import DBDetails.DataFormat;
import DBDetails.FieldFormat;
import Utilities.ByteUtility;

public class TableRecordDetails {
    public Byte noOfRowIds;
    public DataFormat dataType;
    public Byte[] indexValue;
    public List<Integer> rowIds;
    public short pageHeaderIndex;
    public short pageOffset;
    public int leftPageNo;
    public int rightPageNo;
    int pageNo;
    private BTreeNodeDetail indexNode;


    public TableRecordDetails(short pageHeaderIndex, DataFormat dataType, Byte NoOfRowIds, byte[] indexValue, List<Integer> rowIds
            , int leftPageNo, int rightPageNo, int pageNo, short pageOffset) {

        this.pageOffset = pageOffset;
        this.pageHeaderIndex = pageHeaderIndex;
        this.noOfRowIds = NoOfRowIds;
        this.dataType = dataType;
        this.indexValue = ByteUtility.byteToBytes(indexValue);
        this.rowIds = rowIds;

        indexNode = new BTreeNodeDetail(new FieldFormat(this.dataType, indexValue), rowIds);
        this.leftPageNo = leftPageNo;
        this.rightPageNo = rightPageNo;
        this.pageNo = pageNo;
    }

    public BTreeNodeDetail getIndexNode() {
        return indexNode;
    }

}
