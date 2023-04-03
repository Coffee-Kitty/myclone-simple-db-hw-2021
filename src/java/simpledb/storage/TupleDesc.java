package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * 元组schema
     */
    CopyOnWriteArrayList<TDItem> tupleSchema;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tupleSchema.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if(typeAr==null){
            throw new NullPointerException("typeAr is null");
        }
        int typeLen = typeAr.length;
        int fieldLen;
        if(fieldAr==null){
            fieldLen=0;
        }else {
            fieldLen=fieldAr.length;
        }

        if(typeLen<=0||typeLen<fieldLen){//typeAr非法 或者 typeAr长度大于fieldAr则异常
            throw new IllegalArgumentException();
        }
        tupleSchema=new CopyOnWriteArrayList<>();
        int i=0;
        for(;i<typeLen&&i<fieldLen;i++){
            tupleSchema.add(new TDItem(typeAr[i],fieldAr[i]));
        }
        for(;i<typeLen;i++){
            tupleSchema.add(new TDItem(typeAr[i],null));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr,null);//用this(参数列表)的形式，自动调用对应的构造方法。不可以直接使用类名进行调用。
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tupleSchema.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>=this.numFields()){
            throw new NoSuchElementException();
        }
        return tupleSchema.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i<0||i>=this.numFields()){
            throw new NoSuchElementException();
        }
        return tupleSchema.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        int i=0;
        for(;i<tupleSchema.size();i++){
            String fieldName=tupleSchema.get(i).fieldName;
            if((fieldName!=null&&fieldName.equals(name))||name==null&&fieldName==null){
                break;
            }
        }
        if(i==tupleSchema.size()){
            throw new NoSuchElementException();
        }
        return i;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int len=0;
        for(TDItem tdItem:tupleSchema){
            int fieldTypeLen = tdItem.fieldType.getLen();
            len+=fieldTypeLen;
        }
        return len;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        if(td1==null){
            return td2;
        }
        if (td2==null){
            return td1;
        }

        int numFields1= td1.numFields();
        int numFields2= td2.numFields();
        Type[] fieldType = new Type[numFields1 + numFields2];
        String[] fieldName = new String[numFields1 + numFields2];
        int i=0,j=0;
        while(i<numFields1) {
            fieldType[i]=td1.tupleSchema.get(i).fieldType;
            fieldName[i]=td1.tupleSchema.get(i).fieldName;
            i++;
        }
        while(j<numFields2){
            fieldType[i]=td2.tupleSchema.get(j).fieldType;
            fieldName[i]=td2.tupleSchema.get(j).fieldName;
            i++;
            j++;
        }
        return  new TupleDesc(fieldType,fieldName);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(!(o instanceof TupleDesc)){
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        if(this.numFields()!=other.numFields() ||
                this.getSize()!=other.getSize()){
            return false;
        }
        for(int i=0;i<this.numFields();i++){
            if(!this.getFieldType(i).equals(other.getFieldType(i))){
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
       // throw new UnsupportedOperationException("unimplemented");
        StringBuilder hash= new StringBuilder();
        for (TDItem next : tupleSchema) {
            hash.append(next.fieldType)
                    .append(next.fieldName);
        }
        return hash.toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder();
        for(int i=0;i<numFields();i++){
            TDItem tdItem = tupleSchema.get(i);
            stringBuilder.append(tdItem.fieldType.toString())
                    .append("(").append(tdItem.fieldName).append("),");
        }
        stringBuilder.deleteCharAt(numFields()-1);
        return stringBuilder.toString();
    }
}
