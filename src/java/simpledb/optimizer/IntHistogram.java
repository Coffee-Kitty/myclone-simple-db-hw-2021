package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */

/**
 * 此处IntHistogram就维护简单的  定width的桶
 */
public class IntHistogram {

    //所有桶 桶中存放符合范围的元组数
    private final int[] buckets;
    //桶宽度
    private final double width;
    //所有的桶一共多少个元组
    private int tupleCounts;
    //最大最小值范围
    private final int min,max;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min=min;
        this.max=max;
        //共有buckets个桶
        this.buckets=new int[buckets];
        //每个桶的宽度是 (max-min)/buckets
        this.width= Math.max((max - min)*1.0/buckets,1);
        //宽度得小于1
        // 否则下面count+= (v - (min+ indexV*width)) * buckets[indexV]/width;值总是太大  过不了
        tupleCounts=0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public int getIndexV(int v){
        if(v<min||v>max){
            return -1;
        }
        return (int) Math.min((v-min) / width,buckets.length-1);//基本是左闭右开   但最后一个左闭右开 例如100默认放在[99,100)桶
    }
    public void addValue(int v) {
    	// some code goes here
        int indexV = getIndexV(v);
        if(indexV==-1)return;
        buckets[indexV]++;
        tupleCounts++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    //selectivity即 符合条件的元组数/总元组数
    public double estimateSelectivity(Predicate.Op op, int v) {
        double count=0;
        if(op.toString().equals("<")){
            if(v<min)count=0;
            else if(v>max)count=tupleCounts;
            else{
                int indexV = getIndexV(v);
                for (int i = 0; i < indexV; i++) {
                    count+=buckets[i];
                }
                count+= (v - (min+ indexV*width)) * buckets[indexV]/width;
            }
            return count/tupleCounts;
        }else if(op.toString().equals("<=")){
            return estimateSelectivity(Predicate.Op.LESS_THAN,v+1);
        }else if(op.toString().equals(">")){
            if(v<min)count=tupleCounts;
            else if(v>max)count=0;
            else{
                int indexV = getIndexV(v);
                for (int i = indexV+1; i < buckets.length; i++) {
                    count+=buckets[i];
                }
                count+= (min+ (indexV+1)*width-v) * buckets[indexV]/width;
            }
            return count/tupleCounts;
        }else if(op.toString().equals(">=")){
            return 1.0-estimateSelectivity(Predicate.Op.LESS_THAN,v);
        }else if (op.toString().equals("=")){
            //等于可能不止一条
            return estimateSelectivity(Predicate.Op.LESS_THAN,v+1)-estimateSelectivity(Predicate.Op.LESS_THAN,v);
        }else if (op.toString().equals("<>")){
            return 1.0-estimateSelectivity(Predicate.Op.EQUALS,v);
        }


    	// some code goes here
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here

        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + Arrays.toString(buckets) +
                ", width=" + width +
                ", tupleCounts=" + tupleCounts +
                ", min=" + min +
                ", max=" + max +
                '}';
    }
}
