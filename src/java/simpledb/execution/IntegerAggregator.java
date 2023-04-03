package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */

/**
 * 分组字段   聚合字段
 * 例如select sum(money) from table group by id
 *      中id就是分组字段  money就是聚合字段
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //分组字段  在传递的元组中的位置    没有分组则为NO_GROUPING -1
    private int gbFieldIndex;
    //分组字段类型
    private Type gbFieldType;
    //聚合字段 在元组中位置
    private int aField;
    //聚合操作符 如sum min
    private Op what;
    //聚合处理器
    private AggHandler aggHandler;

    //自定义 聚合处理抽象类
    private abstract class AggHandler{
        //定义一个map存储聚合结果
        //key 为分组字段    根据不同的分组 进行各自的聚合才对   没有分组则为NO_GROUPING -1
        //value 为各种操作后的所得值  如sum min等
        final ConcurrentHashMap<Field, Integer> aggResult;
        //处理方法 子类实现
        //参数为分组字段  及 处理的聚合字段
        abstract void handle(Field gbField,IntField aField);
        AggHandler(){
            aggResult=new ConcurrentHashMap<>();
        }
        public ConcurrentHashMap<Field, Integer> getAggResult(){
            return aggResult;
        }
    }
    private class SumAggHandler extends AggHandler{
        @Override
        void handle(Field gbField,IntField aField) {

            aggResult.put(gbField, aggResult.getOrDefault(gbField, 0) + (Integer) aField.getValue());
        }
    }
    private class CountAggHandler extends AggHandler{
        @Override
        void handle(Field gbField,IntField aField) {

            aggResult.put(gbField,aggResult.getOrDefault(gbField,0)+1);
        }
    }
    private class MaxAggHandler extends AggHandler{
        @Override
        void handle(Field gbField,IntField aField) {
            aggResult.put(gbField,Math.max(aggResult.getOrDefault(gbField,Integer.MIN_VALUE),(Integer) aField.getValue()));
        }
    }
    private class MinAggHandler extends AggHandler{
        @Override
        void handle(Field gbField,IntField aField) {
            aggResult.put(gbField,Math.min(aggResult.getOrDefault(gbField,Integer.MAX_VALUE),(Integer) aField.getValue()));
        }
    }
    private class AvgAggHandler extends AggHandler{
        //各组已经处理了多少个字段
//       private final HashMap<Field,Integer> count=new HashMap<>();
//        //此处需注意这里返回的平均数肯定被截断了
//        @Override
//        void handle(Field gbField,IntField aField) {
//            if(aggResult.containsKey(gbField)){
//                Integer value = aField.getValue();
//                Integer old = aggResult.get(gbField);
//                int oldCount= count.get(gbField);
//                int newValue = (old * oldCount + value) / (oldCount + 1); //存在问题  如果截断了怎么办？？？
//                count.put(gbField,oldCount+1);
//                aggResult.put(gbField,newValue);
//            }else{
//                aggResult.put(gbField,aField.getValue());
//                count.put(gbField,1);
//            }
//        }
        private final SumAggHandler sumAggHandler;
        private final CountAggHandler countAggHandler;
        AvgAggHandler(){
            sumAggHandler=new SumAggHandler();
            countAggHandler=new CountAggHandler();
        }
        @Override
        void handle(Field gbField,IntField aField) {
            sumAggHandler.handle(gbField,aField);
            countAggHandler.handle(gbField,aField);
            int newField = sumAggHandler.getAggResult().get(gbField) / countAggHandler.getAggResult().get(gbField);
            this.aggResult.put(gbField, newField);
        }


    }
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIndex=gbfield;
        this.gbFieldType=gbfieldtype;
        this.aField=afield;
        this.what=what;
        switch (what){
            case AVG:
                this.aggHandler=new AvgAggHandler();
                break;
            case MAX:
                this.aggHandler=new MaxAggHandler();
                break;
            case SUM:
                this.aggHandler=new SumAggHandler();
                break;
            case MIN:
                this.aggHandler=new MinAggHandler();
                break;
            case COUNT:
                this.aggHandler=new CountAggHandler();
                break;
            default:
                throw new IllegalArgumentException("聚合器不支持当前运算符");

        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //是否进行分组
        Field gb;
        if(gbFieldIndex==NO_GROUPING){
            gb = new IntField(NO_GROUPING);
        }else{
            gb=tup.getField(gbFieldIndex);
        }
        //查看分组类型是否匹配
        if(gbFieldType!=null&&!(gb.getType().equals(gbFieldType))){
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        //得到聚合字段
        IntField agg = (IntField) tup.getField(aField);

        aggHandler.handle(gb,agg);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    //返回GBHandler中聚合结果的迭代器
    //肯定返回一个TupleIterator就行
    public OpIterator iterator() {
        // some code goes here
        Type[] types;
        String[] names;
        TupleDesc tupleDesc;
        Tuple tuple;
        ConcurrentHashMap<Field, Integer> aggResult = aggHandler.getAggResult();
        //存储Tuple
        List<Tuple> tupleList=new ArrayList<>();
        //如果未进行分组  返回 int(TupleDesc)  聚集值
        if(gbFieldIndex==NO_GROUPING){
            types=new Type[]{Type.INT_TYPE};
               names = new String[]{"aggregateVal"};
            tupleDesc = new TupleDesc(types, names);

            tuple=new Tuple(tupleDesc);
            Integer integer = aggResult.get(new IntField(NO_GROUPING));
            IntField intField = new IntField(integer);
            tuple.setField(0,intField);
            tupleList.add(tuple);
        }else{
            types=new Type[]{gbFieldType,Type.INT_TYPE};
            names=new String[]{"groupVal","aggregateVal"};
            tupleDesc = new TupleDesc(types, names);

            for (Field field : aggResult.keySet()) {
                tuple=new Tuple(tupleDesc);
                //先添加 被分组字段
                if(gbFieldType == Type.INT_TYPE){
                    IntField intField = (IntField) field;
                    tuple.setField(0,intField);
                }else if(gbFieldType == Type.STRING_TYPE){
                    StringField stringField = (StringField) field;
                    tuple.setField(0,stringField);
                }
                //接着添加 聚集字段
                Integer integer = aggResult.get(field);
                tuple.setField(1,new IntField(integer));

                //然后添加到集合
                tupleList.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc,tupleList);
        //如果进行了分组  返回 分组类型(TupleDesc)  聚集值
        //throw new UnsupportedOperationException("please implement me for lab2");
    }

}
