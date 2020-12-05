

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableRecords {
    public int row_ID;
    public Byte[] colDatatypes;
    public Byte[] recordBody;
    private List<FieldFormat> fields;
    public short recordOffset;
    public short pageHeadIndex;

    public TableRecords(short pageHeaderIndex, int rowId, short recordOffset, byte[] colDatatypes, byte[] recordBody) {
        this.row_ID = rowId;
        this.recordBody = ByteUtility.byteToBytes(recordBody);
        this.colDatatypes = ByteUtility.byteToBytes(colDatatypes);
        this.recordOffset = recordOffset;
        this.pageHeadIndex = pageHeaderIndex;
        setAttributes();
    }

    public List<FieldFormat> getFields() {
        return fields;
    }

    private void setAttributes() {
        fields = new ArrayList<>();
        int pointer = 0;
        for (Byte colDataType : colDatatypes) {
            byte[] fieldValue = ByteUtility.Bytestobytes(Arrays.copyOfRange(recordBody, pointer, pointer + DataFormat.getLength(colDataType)));
            fields.add(new FieldFormat(DataFormat.get(colDataType), fieldValue));
            pointer = pointer + DataFormat.getLength(colDataType);
        }
    }
}
