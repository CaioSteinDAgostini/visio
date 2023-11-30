package data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public class Tuple implements Serializable{//, Comparable<Tuple> {

    private Table table;
    private Integer row;
    private Boolean valid;

    protected Tuple() {
        this.valid = false;
    }

    public int getRow() {
        return this.row;
    }

    public <T> T get(String field) throws InvalidFieldException {
        return (T) table.get(row, field);
    }

    public Class getType(String field) {
        return table.getSchema().getFieldType(field);
    }

    public void set(String field, final Object value) throws InvalidFieldException, KeyCollisionException {
        try {
            table.set(this, field, value);
        }
        catch (InvalidTupleException e) {
            throw new InvalidFieldException();
        }
    }

    protected void setUnchecked(String field, Object value) throws InvalidFieldException {
        Column column = table.getColumn(field);
        column.set(row, value);
    }

    public boolean isValid() {
        return this.valid;
    }

    protected void setValid(final boolean condition) {
        this.valid = condition;
    }

    protected void setRow(int row) {
        this.row = row;
    }

    protected void setTable(Table table) {
        this.table = table;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Tuple other = (Tuple) obj;
        if (!Objects.equals(this.table, other.table)) {
            return false;
        }
        if (!Objects.equals(this.row, other.row)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return this.getRow();
    }

    @Override
    public String toString() {
        try {
            StringBuilder sb = new StringBuilder();

            sb.append(this.row).append(":(");
            Iterator<String> fields = this.getTable().getFieldsNames();
            String tmp;
            while (fields.hasNext()) {
                try {
                    Object obj = this.get(fields.next());
                    if(obj!=null){
                        if(obj.getClass().isArray()){
                            sb.append(Arrays.toString((Object[])obj));
                        }else{
                            tmp = obj.toString();
                            sb.append(tmp);
                        }
                    }
                }
                catch (InvalidFieldException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if (fields.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(")");
            return sb.toString();

//            sb.append(this.row + ":(");
//            Iterator<String> fieldsit = this.table.getSchema().getFields();
//            boolean hasNext = fieldsit.hasNext();
//            while (hasNext) {
//                c = this.table.getColumn(fieldsit.next());
//                sb.append(c.getType().getSimpleName() + ":" + c.get(this.row));
//                hasNext = fieldsit.hasNext();
//                if (hasNext) {
//                    sb.append(", ");
//                }
//            }
//            sb.append(")");
//            return sb.toString(); //substring tira o "," no fim
        }
        catch (IndexOutOfBoundsException e) {
            return "";
        }
    }

    public Table getTable() {
        return this.table;
    }

//    @Override
//    public int compareTo(final Tuple tuple) {
//        return this.row - tuple.getRow();
//    }
}
