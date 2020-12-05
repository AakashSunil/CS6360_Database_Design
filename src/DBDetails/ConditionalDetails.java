package DBDetails;

public class ConditionalDetails {
    public String columnName;
    private OperatorsEnum operator;
    public String comparisonValue;
    boolean negation;
    public int column_Ordinal;
    public DataFormat dataType;

    public ConditionalDetails(DataFormat dataType) {
        this.dataType = dataType;
    }

    public static String[] supportedOperators = {"<=", ">=", "<>", ">", "<", "="};

    public static OperatorsEnum getOperatorType(String strOperator) {
        switch (strOperator) {
            case ">":
                return OperatorsEnum.GREATERTHAN;
            case "<":
                return OperatorsEnum.LESSTHAN;
            case "=":
                return OperatorsEnum.EQUALTO;
            case ">=":
                return OperatorsEnum.GREATERTHANOREQUAL;
            case "<=":
                return OperatorsEnum.LESSTHANOREQUAL;
            case "<>":
                return OperatorsEnum.NOTEQUAL;
            default:
                System.out.println("Invalid operator \"" + strOperator + "\"");
                return OperatorsEnum.INVALID;
        }
    }

    public static int compare(String value1, String value2, DataFormat dataType) {
        if (dataType == DataFormat.TEXT)
            return value1.toLowerCase().compareTo(value2);
        else if (dataType == DataFormat.NULL) {
            if (value1 == value2)
                return 0;
            else if (value1.toLowerCase().equals("null"))
                return 1;
            else
                return -1;
        } else {
            return Long.valueOf(Long.parseLong(value1) - Long.parseLong(value2)).intValue();
        }
    }

    private boolean doOperationOnDifference(OperatorsEnum operation, int difference) {
        switch (operation) {
            case LESSTHANOREQUAL:
                return difference <= 0;
            case GREATERTHANOREQUAL:
                return difference >= 0;
            case NOTEQUAL:
                return difference != 0;
            case LESSTHAN:
                return difference < 0;
            case GREATERTHAN:
                return difference > 0;
            case EQUALTO:
                return difference == 0;
            default:
                return false;
        }
    }

    private boolean doStringCompare(String currentValue, OperatorsEnum operation) {
        return doOperationOnDifference(operation, currentValue.toLowerCase().compareTo(comparisonValue));
    }

    public boolean check(String currentValue) {
        OperatorsEnum operation = getOperation();

        if (currentValue.toLowerCase().equals("null")
                || comparisonValue.toLowerCase().equals("null"))
            return doOperationOnDifference(operation, compare(currentValue, comparisonValue, DataFormat.NULL));

        if (dataType == DataFormat.TEXT || dataType == DataFormat.NULL)
            return doStringCompare(currentValue, operation);
        else {

            switch (operation) {
                case LESSTHANOREQUAL:
                    return Long.parseLong(currentValue) <= Long.parseLong(comparisonValue);
                case GREATERTHANOREQUAL:
                    return Long.parseLong(currentValue) >= Long.parseLong(comparisonValue);

                case NOTEQUAL:
                    return Long.parseLong(currentValue) != Long.parseLong(comparisonValue);
                case LESSTHAN:
                    return Long.parseLong(currentValue) < Long.parseLong(comparisonValue);

                case GREATERTHAN:
                    return Long.parseLong(currentValue) > Long.parseLong(comparisonValue);
                case EQUALTO:
                    return Long.parseLong(currentValue) == Long.parseLong(comparisonValue);

                default:
                    return false;

            }

        }

    }

    public void setConditionValue(String conditionValue) {
        this.comparisonValue = conditionValue;
        this.comparisonValue = comparisonValue.replace("'", "");
        this.comparisonValue = comparisonValue.replace("\"", "");

    }

    public void setColumName(String columnName) {
        this.columnName = columnName;
    }

    public void setOperator(String operator) {
        this.operator = getOperatorType(operator);
    }

    public void setNegation(boolean negate) {
        this.negation = negate;
    }

    public OperatorsEnum getOperation() {
        if (!negation)
            return this.operator;
        else
            return negateOperator();
    }

    private OperatorsEnum negateOperator() {
        switch (this.operator) {
            case LESSTHANOREQUAL:
                return OperatorsEnum.GREATERTHAN;
            case GREATERTHANOREQUAL:
                return OperatorsEnum.LESSTHAN;
            case NOTEQUAL:
                return OperatorsEnum.EQUALTO;
            case LESSTHAN:
                return OperatorsEnum.GREATERTHANOREQUAL;
            case GREATERTHAN:
                return OperatorsEnum.LESSTHANOREQUAL;
            case EQUALTO:
                return OperatorsEnum.NOTEQUAL;
            default:
                System.out.println("Invalid operator \"" + this.operator + "\"");
                return OperatorsEnum.INVALID;
        }
    }
}
