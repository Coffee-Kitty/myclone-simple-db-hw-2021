package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    //操作符  Filter Join等
    //// 需要聚合的 tuples
    private OpIterator child;
    //分组字段
    private int gField;
    //聚集字段
    private int aField;
    //聚集符Op  sum min 等
    private Aggregator.Op aop;
    //聚集处理器
    private Aggregator aggregator;
    // 聚合结果的迭代器
    private OpIterator opIterator;
    // 聚合结果的属性行
    private TupleDesc tupleDesc;
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.aField=afield;
        this.child=child;
        this.gField=gfield;
        this.aop=aop;
        //查看是否分组
        Type gfieldType;
        if(gfield==Aggregator.NO_GROUPING){
            gfieldType = null;
        }else{
            gfieldType = child.getTupleDesc().getFieldType(gfield);
        }
        //聚合字段类型
        Type aFieldType = child.getTupleDesc().getFieldType(afield);
        //创建聚合字段对应聚合器
        if(aFieldType==Type.INT_TYPE){
            aggregator=new IntegerAggregator(gfield,gfieldType,afield,aop);
        }else if(aFieldType==Type.STRING_TYPE){
            aggregator=new StringAggregator(gfield,gfieldType,afield,aop);
        }

        // 组建 TupleDesc
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();
        if(gfieldType != null){
            typeList.add(gfieldType);
            nameList.add(child.getTupleDesc().getFieldName(gfield));
        }
        typeList.add(child.getTupleDesc().getFieldType(afield));
        nameList.add(child.getTupleDesc().getFieldName(afield));
        tupleDesc=new TupleDesc(typeList.toArray(new Type[typeList.size()]),nameList.toArray(new String[nameList.size()]));

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    //return the groupby field
    public int groupField() {
        // some code goes here
//        return gField==-1?Aggregator.NO_GROUPING:gField;
        return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    //打开的时候处理所有的 child 变成聚合结果字段
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        //聚合所有的tuple
        child.open();
        while(child.hasNext()){
            Tuple next = child.next();
            aggregator.mergeTupleIntoGroup(next);
        }
        //获取聚合结果的迭代器
        opIterator=aggregator.iterator();
        //查询
        opIterator.open();
        //保持父状态一致
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(opIterator.hasNext()){
            return opIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void close() {
        // some code goes here
        opIterator.close();
        child.close();
        super.close();
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
        Type gfieldtype = child.getTupleDesc().getFieldType(gField);

        // 组建 TupleDesc
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        // 加入分组后的字段
        if(gfieldtype != null){
            typeList.add(gfieldtype);
            nameList.add(child.getTupleDesc().getFieldName(gField));
        }

        // 加入聚合字段
        typeList.add(child.getTupleDesc().getFieldType(aField));
        nameList.add(child.getTupleDesc().getFieldName(aField));

//        if(aop.equals(Aggregator.Op.SUM_COUNT)){
//            typeList.add(Type.INT_TYPE);
//            nameList.add("COUNT");
//        }

        this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]), nameList.toArray(new String[nameList.size()]));
    }

}
