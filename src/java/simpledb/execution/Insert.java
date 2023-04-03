package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;

    // 插入的元组 的 迭代器
    private OpIterator child;
    private int tableId;

    // // 返回的 tuple (用于展示插入了多少的 tuples)
    private TupleDesc tupleDesc;

    //是否插入完毕
    private boolean inserted;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableId);
        if(!tupleDesc.equals(child.getTupleDesc())){
            throw new IllegalArgumentException("TupleDesc of child differs from table");
        }

        this.tableId=tableId;
        this.transactionId=t;
        this.child=child;
        this.tupleDesc=new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"insert_count"});
        inserted=false;

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        //保证父 状态一致
        super.open();

    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        inserted=false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor.
     *
     * It returns a one field tuple containing the number of
     * inserted records.
     *
     * Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool().
     *
     * Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int count=0;
        IntField intField;
        if(!inserted){
            inserted=true;
            //A 1-field tuple containing the number of inserted records
            //插入完毕
            while (child.hasNext()) {
                Tuple next = child.next();
                try {
                    Database.getBufferPool().insertTuple(transactionId, tableId, next);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                count++;
            }
            intField = new IntField(count);
            TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"insert_count"});
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,intField);
            return tuple;
        }

        //or  null if called more than once.
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child=children[0];
    }
}
