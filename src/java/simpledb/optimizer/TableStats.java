package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
/*
给定tableid
针对指定table对其的每一个列建立 histogram
 */
public class TableStats {

    //key 表名
    //value tableStats
    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    //每页的io开销默认值
    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        //获得系统中catalog的指向所有表的指针
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            //将系统catalog中所有表   表名 -> 统计数据放入 statsMap维护
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    //针对每个列  列索引-》对应直方图
    private HashMap<Integer,IntHistogram> intHistogramHashMap;
    private HashMap<Integer,StringHistogram> stringHistogramHashMap;
    private int fieldNums;
    //针对每个列  列索引-》对应列的最大最小值
    private HashMap<Integer,Integer> maxMap;
    private HashMap<Integer,Integer> minMap;
    //桶数 是 NUM_HIST_BINS
    //表中元组总数
    private int totalTuples;
    //表的页数
    private int pagenums;
    private DbFile dbFile;
    private int iocostperpage=IOCOSTPERPAGE;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.iocostperpage=ioCostPerPage;
        intHistogramHashMap=new HashMap<>();
        stringHistogramHashMap=new HashMap<>();
        maxMap=new HashMap<>();
        minMap=new HashMap<>();

        //1.扫描全表，确定每个字段的最大最小值；并且
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.fieldNums = dbFile.getTupleDesc().numFields();
        totalTuples=0;
        this.pagenums=((HeapFile)dbFile).numPages();
        DbFileIterator dbFileIterator = dbFile.iterator(new TransactionId());
        try {
            dbFileIterator.open();
            while (dbFileIterator.hasNext()){
                ++totalTuples;
                Tuple next = dbFileIterator.next();
                for(int i=0; i<fieldNums; i++){
                    if(next.getField(i).getType().equals(Type.INT_TYPE)){
                        IntField field = (IntField) next.getField(i);
                        Integer integer = maxMap.get(i);
                        if(integer!=null){
                            maxMap.put(i,Math.max(field.getValue(),integer));
                        }else {
                            maxMap.put(i,field.getValue());
                        }
                        if(minMap.get(i)!=null) {
                            minMap.put(i, Math.min(field.getValue(), minMap.get(i)));
                        }else {
                            minMap.put(i,field.getValue());
                        }
                    }else{
                        //如果是String类型 则直接构造
                        StringHistogram stringHistogram;
                        if(stringHistogramHashMap.get(i)!=null){
                            stringHistogram = stringHistogramHashMap.get(i);
                        }else{
                            stringHistogram= new StringHistogram(NUM_HIST_BINS);
                            stringHistogramHashMap.put(i,stringHistogram);
                        }
                        stringHistogram.addValue(((StringField) next.getField(i)).getValue());
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        //2.根据最大最小值、表的桶数，构造出每个字段的直方图。.再次扫描全表，并在扫描过程中填充数据。
        try {
            for(int i=0;i<fieldNums;i++){
                if (dbFile.getTupleDesc().getFieldType(i).equals(Type.INT_TYPE)){
                    IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, minMap.get(i), maxMap.get(i));
                    intHistogramHashMap.put(i,intHistogram);
                }
            }

            dbFileIterator.rewind();
            while (dbFileIterator.hasNext()){
                Tuple next = dbFileIterator.next();
                for (int i = 0; i < fieldNums; i++) {
                    if(next.getField(i).getType().equals(Type.INT_TYPE)){
                        IntHistogram intHistogram = intHistogramHashMap.get(i);
                        intHistogram.addValue(((IntField) next.getField(i)).getValue());
                    }
                }
            }


        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }finally {
            dbFileIterator.close();
        }


    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    //返回扫描的io开销  每页的io开销为 IOCOSTPERPAGE
    public double estimateScanCost() {
        // some code goes here
        return 2*pagenums*iocostperpage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    //根据 给定选择性因子 估计结果的记录数：
    //就总元组数 * 选择性因子即可
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (totalTuples*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        if(dbFile.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE)){
            IntHistogram intHistogram = intHistogramHashMap.get(field);
            return intHistogram.avgSelectivity();
        }else{
            StringHistogram stringHistogram = stringHistogramHashMap.get(field);
            return stringHistogram.avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if(dbFile.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE)){
            IntHistogram intHistogram = intHistogramHashMap.get(field);
            return intHistogram.estimateSelectivity(op,((IntField)constant).getValue());
        }else{
            StringHistogram stringHistogram = stringHistogramHashMap.get(field);
            return stringHistogram.estimateSelectivity(op,((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return totalTuples;
    }

}
