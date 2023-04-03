package simpledb;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;
import java.io.*;
public class Test_lab2 {
    public static void main(String[] argv) throws IOException {
        // construct a 3-column table schema
        //System.out.println(System.getProperty("user.dir"));
        //D:\workspace\git-repos\simple-db-hw-2021
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] names = new String[]{"field0", "field1", "field2"};

        TupleDesc td = new TupleDesc(types, names);

        // 将文本转换为二进制文件
        HeapFileEncoder.convert(new File("testLab2_1.dat"), new File("some_data_file1.dat"), BufferPool.getPageSize(), 3, types);
        HeapFileEncoder.convert(new File("testLab2_2.dat"), new File("some_data_file2.dat"), BufferPool.getPageSize(), 3, types);

        // 创建文件 新增表
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");


        // 查询两个表
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // 过滤 table1 的值，过滤条件是 > 1
        Filter sf1 = new Filter(
                new Predicate(0,
                        Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        // 确定要双方要连接的字段
        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // 打印结果
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
