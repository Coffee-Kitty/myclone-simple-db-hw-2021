package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //分组字段类型
    private Type gbFieldType;
    //分组字段
    private int gbField;
    //聚合字段
    private int aField;
    //聚合操作符
    private Op what;
    //聚合器
    private StringAggHandler stringAggHandler;
    //聚合类
    private abstract class StringAggHandler {

        //key->分组的字段 如果没有分类则为 NO_HAVINGGROUP
        //value 因为只做 count
        final  ConcurrentHashMap<String,Integer> aggResult;
        StringAggHandler(){
            this.aggResult=new ConcurrentHashMap<>();
        }
        abstract void handle(String key, Field aField);
        public ConcurrentHashMap<String, Integer> getAggResult(){
            return aggResult;
        }
    }
    private class CountStringAggHandler extends StringAggHandler{
        @Override
        void handle(String key, Field aField) {
            aggResult.put(key,aggResult.getOrDefault(key,0)+1);
        }
    }
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField=gbfield;
        this.gbFieldType=gbfieldtype;
        this.aField=afield;
        this.what=what;
        switch (what){
            case COUNT:
                stringAggHandler=new CountStringAggHandler();
                break;
            default:
                throw new IllegalArgumentException("String聚合器不支持当前运算符");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbFieldType!=null&&(!(tup.getTupleDesc().getFieldType(gbField).equals(gbFieldType)))){
            throw new IllegalArgumentException("Given tuple has wrong type");
        }

        String key;
        if(gbField==NO_GROUPING){
            //未分组
            key = "NO_GROUPING";
        }else{
            key = tup.getField(gbField).toString();
        }

        Field field = tup.getField(aField);
        stringAggHandler.handle(key,field);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
        ConcurrentHashMap<String, Integer> aggResult = stringAggHandler.getAggResult();
        //构建TupleDesc
        Type[]types;
        String[]names;
        TupleDesc tupleDesc;
        Tuple tuple;
        List<Tuple> tupleList=new ArrayList<>();
        //是否分组
        if(gbField==NO_GROUPING){
            types=new Type[]{Type.INT_TYPE};
            names=new String[]{"aggregateVal"};
            tupleDesc=new TupleDesc(types,names);

            tuple=new Tuple(tupleDesc);
            tuple.setField(0,new IntField(aggResult.get("NO_GROUPING")));
            tupleList.add(tuple);
        }else{
            types=new Type[]{gbFieldType,Type.INT_TYPE};
            names=new String[]{"groupVal","aggregateVal"};
            tupleDesc=new TupleDesc(types,names);

            for(String key:aggResult.keySet()){
                tuple=new Tuple(tupleDesc);

                if(gbFieldType==Type.INT_TYPE){
                    tuple.setField(0,new IntField(Integer.parseInt(key)));
                }else if(gbFieldType==Type.STRING_TYPE){
                    tuple.setField(0,new StringField(key,key.length()));
                }
                tuple.setField(1,new IntField(aggResult.get(key)));
                tupleList.add(tuple);
            }

        }

        return new TupleIterator(tupleDesc,tupleList);
    }

}
