package Storage.BTree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import DBDetails.ColumnDetails;
import DBDetails.ConditionalDetails;
import DBDetails.DataFormat;
import DBDetails.FieldFormat;
import DBDetails.MetaData;
import Storage.BinaryFileAccess;
import Storage.TableRecordInteriorWorking;
import Storage.TableRecords;
import Storage.Details.BTreeNodeDetail;
import Storage.Details.TableRecordDetails;
import Utilities.ByteUtility;

public class PageTrack {
    public PageType pageType;
    public short noOfCells = 0;
    public int pageNo;
    short contentStartOffset;
    public int rightPage;
    public int parentPageNo;
    private List<TableRecords> records;
    boolean refreshTableRecords = false;
    long pageStart;
    int lastRowId;
    int availableSpace;
    RandomAccessFile binaryFile;
    public List<TableRecordInteriorWorking> leftChildren;

    public DataFormat indexValueDataType;
    public TreeSet<Long> lIndexValues;
    public TreeSet<String> sIndexValues;
    public HashMap<String, TableRecordDetails> indexValuePointer;
    private Map<Integer, TableRecords> recordsMap;

    public PageTrack(RandomAccessFile file, int pageNo) {
        try {
            this.pageNo = pageNo;
            indexValueDataType = null;
            lIndexValues = new TreeSet<>();
            sIndexValues = new TreeSet<>();
            indexValuePointer = new HashMap<String, TableRecordDetails>();
            recordsMap = new HashMap<>();

            this.binaryFile = file;
            lastRowId = 0;
            pageStart = BinaryFileAccess.pageSize * pageNo;
            binaryFile.seek(pageStart);
            pageType = PageType.get(binaryFile.readByte()); // pagetype
            binaryFile.readByte(); // unused
            noOfCells = binaryFile.readShort();
            contentStartOffset = binaryFile.readShort();
            availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);

            rightPage = binaryFile.readInt();

            parentPageNo = binaryFile.readInt();

            binaryFile.readShort();// 2 unused bytes

            if (pageType == PageType.LEAF)
                fillTableRecords();
            if (pageType == PageType.INTERIOR)
                fillLeftChildren();
            if (pageType == PageType.INTERIORINDEX || pageType == PageType.LEAF_INDEX)
                fillIndexRecords();

        } catch (IOException ex) {
            System.out.println("Error while reading the page " + ex.getMessage());
        }
    }

    public List<String> getIndexValues() {
        List<String> strIndexValues = new ArrayList<>();

        if (sIndexValues.size() > 0)
            strIndexValues.addAll(Arrays.asList(sIndexValues.toArray(new String[sIndexValues.size()])));
        if (lIndexValues.size() > 0) {
            Long[] lArray = lIndexValues.toArray(new Long[lIndexValues.size()]);
            for (int i = 0; i < lArray.length; i++) {
                strIndexValues.add(lArray[i].toString());
            }
        }

        return strIndexValues;


    }

    public boolean isRoot() {
        return parentPageNo == -1;
    }


    public static PageType getPageType(RandomAccessFile file, int pageNo) throws IOException {
        try {
            int pageStart = BinaryFileAccess.pageSize * pageNo;
            file.seek(pageStart);
            return PageType.get(file.readByte());
        } catch (IOException ex) {
            System.out.println("Error while getting the page type " + ex.getMessage());
            throw ex;
        }
    }

    public static int addNewPage(RandomAccessFile file, PageType pageType, int rightPage, int parentPageNo) {
        try {
            int pageNo = Long.valueOf((file.length() / BinaryFileAccess.pageSize)).intValue();
            file.setLength(file.length() + BinaryFileAccess.pageSize);
            file.seek(BinaryFileAccess.pageSize * pageNo);
            file.write(pageType.getValue());
            file.write(0x00); //unused
            file.writeShort(0); // no of cells
            file.writeShort((short) (BinaryFileAccess.pageSize)); // cell start offset

            file.writeInt(rightPage);

            file.writeInt(parentPageNo);

            return pageNo;
        } catch (IOException ex) {
            System.out.println("Error while adding new page" + ex.getMessage());
            return -1;
        }
    }

    public void updateRecord(TableRecords record, int ordinalPosition, Byte[] newValue) throws IOException {
        binaryFile.seek(pageStart + record.recordOffset + 7);
        int valueOffset = 0;
        for (int i = 0; i < ordinalPosition; i++) {

            valueOffset += DataFormat.getLength(binaryFile.readByte());
        }

        binaryFile.seek(pageStart + record.recordOffset + 7 + record.colDatatypes.length + valueOffset);
        binaryFile.write(ByteUtility.Bytestobytes(newValue));

    }


    public void addNewColumn(ColumnDetails column) throws IOException {
        try {
            addTableRow(BinaryFileAccess.columns_Table, Arrays.asList(new FieldFormat(DataFormat.TEXT, column.table_name),
                    new FieldFormat(DataFormat.TEXT, column.column_Name),
                    new FieldFormat(DataFormat.TEXT, column.datatype.toString()),
                    new FieldFormat(DataFormat.SMALLINT, column.ordinalPosition.toString()),
                    new FieldFormat(DataFormat.TEXT, column.isNullable ? "YES" : "NO"),
                    column.isPrimaryKey ?
                            new FieldFormat(DataFormat.TEXT, "PRI") : new FieldFormat(DataFormat.NULL, "NULL"),
                    new FieldFormat(DataFormat.TEXT, column.isUnique ? "YES" : "NO")));
        } catch (Exception e) {
            System.out.println("Could not add column");
        }
    }

    public int addTableRow(String tableName, List<FieldFormat> fields) throws IOException {
        List<Byte> colDataTypes = new ArrayList<Byte>();
        List<Byte> recordBody = new ArrayList<Byte>();

        MetaData metaData = null;
        if (BinaryFileAccess.dataStoreInitialized) {
            metaData = new MetaData(tableName);
            if (!metaData.validateInsert(fields))
                return -1;
        }

        for (FieldFormat field : fields) {
            recordBody.addAll(Arrays.asList(field.fieldValueByte));

            if (field.dataType == DataFormat.TEXT) {
                colDataTypes.add(Integer.valueOf(DataFormat.TEXT.getValue() + (field.fieldValue.length())).byteValue());
            } else {
                colDataTypes.add(field.dataType.getValue());
            }
        }

        lastRowId++;

        short payLoadSize = Integer.valueOf(recordBody.size() +
                colDataTypes.size() + 1).shortValue();

        List<Byte> recordHeader = new ArrayList<>();

        recordHeader.addAll(Arrays.asList(ByteUtility.shortToBytes(payLoadSize)));  //payloadSize
        recordHeader.addAll(Arrays.asList(ByteUtility.intToBytes(lastRowId))); //rowid
        recordHeader.add(Integer.valueOf(colDataTypes.size()).byteValue()); //number of columns
        recordHeader.addAll(colDataTypes); //column data types

        addNewPageRecord(recordHeader.toArray(new Byte[recordHeader.size()]),
                recordBody.toArray(new Byte[recordBody.size()])
        );

        refreshTableRecords = true;
        if (BinaryFileAccess.dataStoreInitialized) {
            metaData.record_count++;
            metaData.updateMetaData();
        }
        return lastRowId;
    }

    public List<TableRecords> getPageRecords() {

        if (refreshTableRecords)
            fillTableRecords();

        refreshTableRecords = false;

        return records;
    }

    private void DeletePageRecord(short recordIndex) {
        try {

            for (int i = recordIndex + 1; i < noOfCells; i++) {
                binaryFile.seek(pageStart + 0x10 + (i * 2));
                short cellStart = binaryFile.readShort();

                if (cellStart == 0)
                    continue;

                binaryFile.seek(pageStart + 0x10 + ((i - 1) * 2));
                binaryFile.writeShort(cellStart);
            }

            noOfCells--;

            binaryFile.seek(pageStart + 2);
            binaryFile.writeShort(noOfCells);

        } catch (IOException e) {
            System.out.println("Error while deleting record at " + recordIndex + "in page " + pageNo);
        }
    }

    public void DeleteTableRec(String tableName, short recordIndex) {
        DeletePageRecord(recordIndex);
        MetaData metaData = new MetaData(tableName);
        metaData.record_count--;
        metaData.updateMetaData();
        refreshTableRecords = true;

    }

    private void addNewPageRecord(Byte[] recordHeader, Byte[] recordBody) throws IOException {

        if (recordHeader.length + recordBody.length + 4 > availableSpace) {
            try {
                if (pageType == PageType.LEAF || pageType == PageType.INTERIOR) {
                    handleTableOverFlow();
                } else {
                    handleIndexOverflow();
                    return;
                }
            } catch (IOException e) {
                System.out.println("Error while handling overflow");
            }
        }

        short cellStart = contentStartOffset;


        short newCellStart = Integer.valueOf((cellStart - recordBody.length - recordHeader.length - 2)).shortValue();
        binaryFile.seek(pageNo * BinaryFileAccess.pageSize + newCellStart);

        //record head
        binaryFile.write(ByteUtility.Bytestobytes(recordHeader)); // datatypes

        //record body
        binaryFile.write(ByteUtility.Bytestobytes(recordBody));

        binaryFile.seek(pageStart + 0x10 + (noOfCells * 2));
        binaryFile.writeShort(newCellStart);

        contentStartOffset = newCellStart;

        binaryFile.seek(pageStart + 4);
        binaryFile.writeShort(contentStartOffset);

        noOfCells++;
        binaryFile.seek(pageStart + 2);
        binaryFile.writeShort(noOfCells);

        availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);

    }

    private boolean idxPageCleaned;

    private void handleIndexOverflow() throws IOException {
        if (pageType == PageType.LEAF_INDEX) {
            if (parentPageNo == -1) {
                parentPageNo = addNewPage(binaryFile, PageType.INTERIORINDEX, pageNo, -1);
            }
            int newLeftLeafPageNo = addNewPage(binaryFile, PageType.LEAF_INDEX, pageNo, parentPageNo);

            setParent(parentPageNo);

            BTreeNodeDetail incomingInsertTemp = this.incomingInsert;

            PageTrack leftLeafPage = new PageTrack(binaryFile, newLeftLeafPageNo);
            BTreeNodeDetail toInsertParentIndexNode = splitIndexRecordsBetweenPages(leftLeafPage);


            PageTrack parentPage = new PageTrack(binaryFile, parentPageNo);

            int comparisonResult = ConditionalDetails.compare(incomingInsertTemp.indexValue.fieldValue, toInsertParentIndexNode.indexValue.fieldValue, incomingInsert.indexValue.dataType);

            if (comparisonResult == 0) {
                toInsertParentIndexNode.rowids.addAll(incomingInsertTemp.rowids);
                parentPage.addIndex(toInsertParentIndexNode, newLeftLeafPageNo);
                shiftPage(parentPage);
                return;
            } else if (comparisonResult < 0) {
                leftLeafPage.addIndex(incomingInsertTemp);
                shiftPage(leftLeafPage);
            } else {
                addIndex(incomingInsertTemp);
            }

            parentPage.addIndex(toInsertParentIndexNode, newLeftLeafPageNo);

        } else {

            if (noOfCells < 3 && !idxPageCleaned) {
                idxPageCleaned = true;
                String[] indexValuesTemp = getIndexValues().toArray(new String[getIndexValues().size()]);
                HashMap<String, TableRecordDetails> indexValuePointerTemp = (HashMap<String, TableRecordDetails>) indexValuePointer.clone();
                BTreeNodeDetail incomingInsertTemp = this.incomingInsert;
                cleanPage();
                for (int i = 0; i < indexValuesTemp.length; i++) {
                    addIndex(indexValuePointerTemp.get(indexValuesTemp[i]).getIndexNode(), indexValuePointerTemp.get(indexValuesTemp[i]).leftPageNo);
                }

                addIndex(incomingInsertTemp);
                return;
            }

            if (idxPageCleaned) {
//                System.out.println("recordupdates.tree.PageTrack overflow, increase the page size. Reached Max number of rows for an Index value");
                return;
            }


            if (parentPageNo == -1) {
                parentPageNo = addNewPage(binaryFile, PageType.INTERIORINDEX, pageNo, -1);
            }
            int newLeftInteriorPageNo = addNewPage(binaryFile, PageType.INTERIORINDEX, pageNo, parentPageNo);


            setParent(parentPageNo);

            BTreeNodeDetail incomingInsertTemp = this.incomingInsert;

            PageTrack leftInteriorPage = new PageTrack(binaryFile, newLeftInteriorPageNo);

            BTreeNodeDetail toInsertParentIndexNode = splitIndexRecordsBetweenPages(leftInteriorPage);

            PageTrack parentPage = new PageTrack(binaryFile, parentPageNo);
            int comparisonResult = ConditionalDetails.compare(incomingInsertTemp.indexValue.fieldValue, toInsertParentIndexNode.indexValue.fieldValue, incomingInsert.indexValue.dataType);


            PageTrack middleOrphan = new PageTrack(binaryFile, toInsertParentIndexNode.leftPageNo);
            middleOrphan.setParent(parentPageNo);
            leftInteriorPage.setRightPageNo(middleOrphan.pageNo);

            if (comparisonResult == 0) {
                toInsertParentIndexNode.rowids.addAll(incomingInsertTemp.rowids);
                parentPage.addIndex(toInsertParentIndexNode, newLeftInteriorPageNo);
                shiftPage(parentPage);
                return;
            } else if (comparisonResult < 0) {
                leftInteriorPage.addIndex(incomingInsertTemp);
                shiftPage(leftInteriorPage);
            } else {
                addIndex(incomingInsertTemp);
            }

            parentPage.addIndex(toInsertParentIndexNode, newLeftInteriorPageNo);

        }


    }

    private void cleanPage() throws IOException {

        noOfCells = 0;
        contentStartOffset = Long.valueOf(BinaryFileAccess.pageSize).shortValue();
        availableSpace = contentStartOffset - 0x10 - (noOfCells * 2); // this page will now be treated as a new page
        byte[] emptybytes = new byte[512 - 16];
        Arrays.fill(emptybytes, (byte) 0);
        binaryFile.seek(pageStart + 16);
        binaryFile.write(emptybytes);
        binaryFile.seek(pageStart + 2);
        binaryFile.writeShort(noOfCells);
        binaryFile.seek(pageStart + 4);
        binaryFile.writeShort(contentStartOffset);
        lIndexValues = new TreeSet<>();
        sIndexValues = new TreeSet<>();
        indexValuePointer = new HashMap<>();

    }


    private BTreeNodeDetail splitIndexRecordsBetweenPages(PageTrack newleftPage) throws IOException {

        try {
            int mid = getIndexValues().size() / 2;
            String[] indexValuesTemp = getIndexValues().toArray(new String[getIndexValues().size()]);

            BTreeNodeDetail toInsertParentIndexNode = indexValuePointer.get(indexValuesTemp[mid]).getIndexNode();
            toInsertParentIndexNode.leftPageNo = indexValuePointer.get(indexValuesTemp[mid]).leftPageNo;

            HashMap<String, TableRecordDetails> indexValuePointerTemp = (HashMap<String, TableRecordDetails>) indexValuePointer.clone();

            for (int i = 0; i < mid; i++) {
                newleftPage.addIndex(indexValuePointerTemp.get(indexValuesTemp[i]).getIndexNode(), indexValuePointerTemp.get(indexValuesTemp[i]).leftPageNo);
            }

            cleanPage();
            sIndexValues = new TreeSet<>();
            lIndexValues = new TreeSet<>();
            indexValuePointer = new HashMap<String, TableRecordDetails>();

            for (int i = mid + 1; i < indexValuesTemp.length; i++) {
                addIndex(indexValuePointerTemp.get(indexValuesTemp[i]).getIndexNode(), indexValuePointerTemp.get(indexValuesTemp[i]).leftPageNo);
            }

            return toInsertParentIndexNode;
        } catch (IOException e) {
            System.out.println("Insert into Index File failed. Error while splitting index pages");
            throw e;
        }

    }

    private void handleTableOverFlow() throws IOException {
        if (pageType == PageType.LEAF) {
            int newRightLeafPageNo = addNewPage(binaryFile, pageType, -1, -1);

            if (parentPageNo == -1) {


                int newParentPageNo = addNewPage(binaryFile, PageType.INTERIOR,
                        newRightLeafPageNo, -1);

                setRightPageNo(newRightLeafPageNo);
                setParent(newParentPageNo);

                PageTrack newParentPage = new PageTrack(binaryFile, newParentPageNo);
                newParentPageNo = newParentPage.addLeftTableChild(pageNo, lastRowId);
                newParentPage.setRightPageNo(newRightLeafPageNo);


                PageTrack newLeafPage = new PageTrack(binaryFile, newRightLeafPageNo);
                newLeafPage.setParent(newParentPageNo);

                shiftPage(newLeafPage);
            } else {

                PageTrack parentPage = new PageTrack(binaryFile, parentPageNo);
                parentPageNo = parentPage.addLeftTableChild(pageNo, lastRowId);


                parentPage.setRightPageNo(newRightLeafPageNo);


                setRightPageNo(newRightLeafPageNo);


                PageTrack newLeafPage = new PageTrack(binaryFile, newRightLeafPageNo);
                newLeafPage.setParent(parentPageNo);


                shiftPage(newLeafPage);
            }
        } else {

            int newRightLeafPageNo = addNewPage(binaryFile, pageType, -1, -1);


            int newParentPageNo = addNewPage(binaryFile, PageType.INTERIOR,
                    newRightLeafPageNo, -1);


            setRightPageNo(newRightLeafPageNo);


            setParent(newParentPageNo);


            PageTrack newParentPage = new PageTrack(binaryFile, newParentPageNo);
            newParentPageNo = newParentPage.addLeftTableChild(pageNo, lastRowId);

            newParentPage.setRightPageNo(newRightLeafPageNo);


            PageTrack newLeafPage = new PageTrack(binaryFile, newRightLeafPageNo);
            newLeafPage.setParent(newParentPageNo);


            shiftPage(newLeafPage);
        }
    }


    private int addLeftTableChild(int leftChildPageNo, int rowId) throws IOException {
        for (TableRecordInteriorWorking intRecord : leftChildren) {
            if (intRecord.rowId == rowId)
                return pageNo;
        }
        if (pageType == PageType.INTERIOR) {
            List<Byte> recordHeader = new ArrayList<>();
            List<Byte> recordBody = new ArrayList<>();

            recordHeader.addAll(Arrays.asList(ByteUtility.intToBytes(leftChildPageNo)));
            recordBody.addAll(Arrays.asList(ByteUtility.intToBytes(rowId)));

            addNewPageRecord(recordHeader.toArray(new Byte[recordHeader.size()]),
                    recordBody.toArray(new Byte[recordBody.size()]));
        }
        return pageNo;

    }


    private void shiftPage(PageTrack newPage) {
        pageType = newPage.pageType;
        noOfCells = newPage.noOfCells;
        pageNo = newPage.pageNo;
        contentStartOffset = newPage.contentStartOffset;
        rightPage = newPage.rightPage;
        parentPageNo = newPage.parentPageNo;
        leftChildren = newPage.leftChildren;
        sIndexValues = newPage.sIndexValues;
        lIndexValues = newPage.lIndexValues;
        indexValuePointer = newPage.indexValuePointer;
        records = newPage.records;
        pageStart = newPage.pageStart;
        availableSpace = newPage.availableSpace;
    }


    public void setParent(int parentPageNo) throws IOException {
        binaryFile.seek(BinaryFileAccess.pageSize * pageNo + 0x0A);
        binaryFile.writeInt(parentPageNo);
        this.parentPageNo = parentPageNo;
    }


    public void setRightPageNo(int rightPageNo) throws IOException {
        binaryFile.seek(BinaryFileAccess.pageSize * pageNo + 0x06);
        binaryFile.writeInt(rightPageNo);
        this.rightPage = rightPageNo;
    }

    public void DeleteIndex(BTreeNodeDetail node) throws IOException {
        DeletePageRecord(indexValuePointer.get(node.indexValue.fieldValue).pageHeaderIndex);
        fillIndexRecords();
        refreshHeaderOffset();
    }

    public void addIndex(BTreeNodeDetail node) throws IOException {
        addIndex(node, -1);
    }

    private BTreeNodeDetail incomingInsert;

    public void addIndex(BTreeNodeDetail node, int leftPageNo) throws IOException {
        incomingInsert = node;
        incomingInsert.leftPageNo = leftPageNo;
        List<Integer> rowIds = new ArrayList<>();


        List<String> ixValues = getIndexValues();
        if (getIndexValues().contains(node.indexValue.fieldValue)) {
            leftPageNo = indexValuePointer.get(node.indexValue.fieldValue).leftPageNo;
            incomingInsert.leftPageNo = leftPageNo;
            rowIds = indexValuePointer.get(node.indexValue.fieldValue).rowIds;
            rowIds.addAll(incomingInsert.rowids);
            incomingInsert.rowids = rowIds;
            DeletePageRecord(indexValuePointer.get(node.indexValue.fieldValue).pageHeaderIndex);
            if (indexValueDataType == DataFormat.TEXT || indexValueDataType == null)
                sIndexValues.remove(node.indexValue.fieldValue);
            else
                lIndexValues.remove(Long.parseLong(node.indexValue.fieldValue));
        }

        rowIds.addAll(node.rowids);

        rowIds = new ArrayList<>(new HashSet<>(rowIds));

        List<Byte> recordHead = new ArrayList<>();
        List<Byte> recordBody = new ArrayList<>();


        recordBody.addAll(Arrays.asList(Integer.valueOf(rowIds.size()).byteValue()));


        if (node.indexValue.dataType == DataFormat.TEXT)
            recordBody.add(Integer.valueOf(node.indexValue.dataType.getValue()
                    + node.indexValue.fieldValue.length()).byteValue());
        else
            recordBody.add(node.indexValue.dataType.getValue());

        recordBody.addAll(Arrays.asList(node.indexValue.fieldValueByte));


        for (int i = 0; i < rowIds.size(); i++) {
            recordBody.addAll(Arrays.asList(ByteUtility.intToBytes(rowIds.get(i))));
        }

        short payload = Integer.valueOf(recordBody.size()).shortValue();
        if (pageType == PageType.INTERIORINDEX)
            recordHead.addAll(Arrays.asList(ByteUtility.intToBytes(leftPageNo)));

        recordHead.addAll(Arrays.asList(ByteUtility.shortToBytes(payload)));

        addNewPageRecord(recordHead.toArray(new Byte[recordHead.size()]),
                recordBody.toArray(new Byte[recordBody.size()])
        );

        fillIndexRecords();
        refreshHeaderOffset();

    }

    private void refreshHeaderOffset() {
        try {
            binaryFile.seek(pageStart + 0x10);
            for (String indexVal : getIndexValues()) {
                binaryFile.writeShort(indexValuePointer.get(indexVal).pageOffset);
            }

        } catch (IOException ex) {
            System.out.println("Error while refrshing header offset " + ex.getMessage());
        }
    }


    private void fillTableRecords() {
        short payLoadSize = 0;
        byte noOfcolumns = 0;
        records = new ArrayList<TableRecords>();
        recordsMap = new HashMap<>();
        try {
            for (short i = 0; i < noOfCells; i++) {
                binaryFile.seek(pageStart + 0x10 + (i * 2));
                short cellStart = binaryFile.readShort();
                if (cellStart == 0)
                    continue;
                binaryFile.seek(pageStart + cellStart);

                payLoadSize = binaryFile.readShort();
                int rowId = binaryFile.readInt();
                noOfcolumns = binaryFile.readByte();

                if (lastRowId < rowId) lastRowId = rowId;

                byte[] colDatatypes = new byte[noOfcolumns];
                byte[] recordBody = new byte[payLoadSize - noOfcolumns - 1];

                binaryFile.read(colDatatypes);
                binaryFile.read(recordBody);

                TableRecords record = new TableRecords(i, rowId, cellStart
                        , colDatatypes, recordBody);
                records.add(record);
                recordsMap.put(rowId, record);
            }
        } catch (IOException ex) {
            System.out.println("Error while filling records from the page " + ex.getMessage());
        }
    }

    private void fillLeftChildren() {
        try {
            leftChildren = new ArrayList<>();

            int leftChildPageNo = 0;
            int rowId = 0;
            for (int i = 0; i < noOfCells; i++) {
                binaryFile.seek(pageStart + 0x10 + (i * 2));
                short cellStart = binaryFile.readShort();
                if (cellStart == 0)//ignore deleted cells
                    continue;
                binaryFile.seek(pageStart + cellStart);

                leftChildPageNo = binaryFile.readInt();
                rowId = binaryFile.readInt();
                leftChildren.add(new TableRecordInteriorWorking(rowId, leftChildPageNo));
            }
        } catch (IOException ex) {
            System.out.println("Error while filling records from the page " + ex.getMessage());
        }

    }

    private void fillIndexRecords() {
        try {
            lIndexValues = new TreeSet<>();
            sIndexValues = new TreeSet<>();
            indexValuePointer = new HashMap<>();

            int leftPageNo = -1;
            byte noOfRowIds = 0;
            byte dataType = 0;
            for (short i = 0; i < noOfCells; i++) {
                binaryFile.seek(pageStart + 0x10 + (i * 2));
                short cellStart = binaryFile.readShort();
                if (cellStart == 0)//ignore deleted cells
                    continue;
                binaryFile.seek(pageStart + cellStart);

                if (pageType == PageType.INTERIORINDEX)
                    leftPageNo = binaryFile.readInt();

                short payload = binaryFile.readShort(); // payload

                noOfRowIds = binaryFile.readByte();
                dataType = binaryFile.readByte();

                if (indexValueDataType == null && DataFormat.get(dataType) != DataFormat.NULL)
                    indexValueDataType = DataFormat.get(dataType);

                byte[] indexValue = new byte[DataFormat.getLength(dataType)];
                binaryFile.read(indexValue);

                List<Integer> lstRowIds = new ArrayList<>();
                for (int j = 0; j < noOfRowIds; j++) {
                    lstRowIds.add(binaryFile.readInt());
                }

                TableRecordDetails record = new TableRecordDetails(i, DataFormat.get(dataType), noOfRowIds, indexValue
                        , lstRowIds, leftPageNo, rightPage, pageNo, cellStart);

                if (indexValueDataType == DataFormat.TEXT || indexValueDataType == null)
                    sIndexValues.add(record.getIndexNode().indexValue.fieldValue);
                else
                    lIndexValues.add(Long.parseLong(record.getIndexNode().indexValue.fieldValue));

                indexValuePointer.put(record.getIndexNode().indexValue.fieldValue, record);

            }
        } catch (IOException ex) {
            System.out.println("Error while filling records from the page " + ex.getMessage());
        }
    }
}
