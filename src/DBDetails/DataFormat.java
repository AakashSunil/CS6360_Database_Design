package DBDetails;

import java.util.HashMap;
import java.util.Map;

public enum DataFormat {
    NULL((byte) 0) {
        @Override
        public String toString() {
            return "NULL";
        }
    },
    TINYINT((byte) 1) {
        @Override
        public String toString() {
            return "TINYINT";
        }
    },
    SMALLINT((byte) 2) {
        @Override
        public String toString() {
            return "SMALLINT";
        }
    },
    INT((byte) 3) {
        @Override
        public String toString() {
            return "INT";
        }
    },
    BIGINT((byte) 4) {
        @Override
        public String toString() {
            return "BIGINT";
        }
    },
    FLOAT((byte) 5) {
        @Override
        public String toString() {
            return "FLOAT";
        }
    },
    DOUBLE((byte) 6) {
        @Override
        public String toString() {
            return "DOUBLE";
        }
    },
    YEAR((byte) 8) {
        @Override
        public String toString() {
            return "YEAR";
        }
    },
    TIME((byte) 9) {
        @Override
        public String toString() {
            return "TIME";
        }
    },
    DATETIME((byte) 10) {
        @Override
        public String toString() {
            return "DATETIME";
        }
    },
    DATE((byte) 11) {
        @Override
        public String toString() {
            return "DATE";
        }
    },
    TEXT((byte) 12) {
        @Override
        public String toString() {
            return "TEXT";
        }
    };


    private static final Map<Byte, DataFormat> dataTypeLookup = new HashMap<Byte, DataFormat>();
    private static final Map<Byte, Integer> dataTypeSizeLookup = new HashMap<Byte, Integer>();
    private static final Map<String, DataFormat> dataTypeStringLookup = new HashMap<String, DataFormat>();
    private static final Map<DataFormat, Integer> dataTypePrintOffset = new HashMap<DataFormat, Integer>();


    static {
        for (DataFormat s : DataFormat.values()) {
            dataTypeLookup.put(s.getValue(), s);
            dataTypeStringLookup.put(s.toString(), s);

            if (s == DataFormat.TINYINT || s == DataFormat.YEAR) {
                dataTypeSizeLookup.put(s.getValue(), 1);
                dataTypePrintOffset.put(s, 6);
            } else if (s == DataFormat.SMALLINT) {
                dataTypeSizeLookup.put(s.getValue(), 2);
                dataTypePrintOffset.put(s, 8);
            } else if (s == DataFormat.INT || s == DataFormat.FLOAT || s == DataFormat.TIME) {
                dataTypeSizeLookup.put(s.getValue(), 4);
                dataTypePrintOffset.put(s, 10);
            } else if (s == DataFormat.BIGINT || s == DataFormat.DOUBLE
                    || s == DataFormat.DATETIME || s == DataFormat.DATE) {
                dataTypeSizeLookup.put(s.getValue(), 8);
                dataTypePrintOffset.put(s, 25);
            } else if (s == DataFormat.TEXT) {
                dataTypePrintOffset.put(s, 25);
            } else if (s == DataFormat.NULL) {
                dataTypeSizeLookup.put(s.getValue(), 0);
                dataTypePrintOffset.put(s, 6);
            }
        }


    }

    private byte value;

    DataFormat(byte value) {
        this.value = value;
    }


    public byte getValue() {
        return value;
    }

    public static DataFormat get(byte value) {
        if (value > 12)
            return DataFormat.TEXT;
        return dataTypeLookup.get(value);
    }

    public static DataFormat get(String text) {
        return dataTypeStringLookup.get(text);
    }

    public static int getLength(DataFormat type) {
        return getLength(type.getValue());
    }

    public static int getLength(byte value) {
        if (get(value) != DataFormat.TEXT)
            return dataTypeSizeLookup.get(value);
        else
            return value - 12;
    }

    public int getPrintOffset() {
        return dataTypePrintOffset.get(get(this.value));
    }


}