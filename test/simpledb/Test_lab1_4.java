package simpledb;

import org.junit.Test;
import simpledb.common.*;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class Test_lab1_4 {


    /**
     * 	1.创建表
     * 	显然，我们需要指定  tupleDesc（表头）  表名  主键名
     * 	同时还有指定的物理文件HeapFile
     * 	然后调用Catalog的addTable方法即可
     */
    @Test
     public void testCreate() throws IOException {
        Catalog catalog = Database.getCatalog();
        //1.指定表头  表schema
        String[] names = {"id", "age", "name"};
        Type[] types = {Type.INT_TYPE, Type.INT_TYPE, Type.STRING_TYPE};
        TupleDesc tupleDesc = new TupleDesc(types,names);

        //创建表的物理文件  注意  表文件或者说数据库文件  需要.dat 二进制文件
        //将.txt转换为.dat
        File infile = new File("student.txt");
        File outfile = new File("student.dat");
        if(!infile.exists()){
            boolean infileNewFile = infile.createNewFile();
            System.out.println(infileNewFile);
        }
        if(!outfile.exists()){
            boolean outfileNewFile = outfile.createNewFile();
            System.out.println(outfileNewFile);
        }
        HeapFileEncoder.convert(infile,outfile, BufferPool.getPageSize(),3);

        //2.创建表存储的物理文件
        HeapFile heapFile = new HeapFile(outfile, tupleDesc);

        //3.将表文件添加到数据库  并且指定表名称  和 主键名称
        catalog.addTable(heapFile,"student_table","id");

    }

    /**
     * 添加一行数据
     *  		需指定:
     *  					表名
     *  					Tuple
     *  		我们通过catelog来获取表明对应的表id
     *  		然后根据表id调用缓冲区的insertTuple
     *  		缓冲区又会调用DbFile的插入
     *  					DbFile获取缓存区中页，没有缓存区会自动读磁盘
     *  					然后写到缓冲页上，返回脏页给调用者
     *  		插入完成后，接着我们显示调用flush刷新到磁盘吧
     */

    @Test
    public void insert() throws IOException, TransactionAbortedException, DbException {
        //调用更改过的create方法  将创建的test表给数据库的catalog管理
        testCreate();
        //1.指定表明  tuple
        String name="student_table";
        String[] names = {"id", "age", "name"};
        Type[] types = {Type.INT_TYPE, Type.INT_TYPE, Type.STRING_TYPE};
        TupleDesc tupleDesc = new TupleDesc(types,names);
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0,new IntField(1));
        tuple.setField(1,new IntField(18));
        tuple.setField(2,new StringField("xiaoming",10));

        //2.通过catelog获取表id
        Catalog catalog = Database.getCatalog();
        int tableId = catalog.getTableId(name);
        //3.根据表id调用缓冲区来插入
        //                  缓冲区又会调用DbFile的插入
        //     *  					DbFile获取缓存区中页，没有缓存区会自动读磁盘
        //     *  					然后写到缓冲页上，返回脏页给调用者
        BufferPool bufferPool = Database.getBufferPool();
        bufferPool.insertTuple(new TransactionId(),tableId,tuple);
        //4.LRU缓存已经被标记为脏页   但还未刷新到磁盘
        bufferPool.flushAllPages();
    }


    /**
     * 	3.获取表中所有数据
     * 	catelog通过表名获取表id，
     * 	catelog根据表id获取对应的表文件
     * 	heapFile调用numpages获取一共多少页
     *
     * 	bufferpool调用getPage获取所有的页
     * 						如果LRU缓存不存在，则从磁盘去读
     * 						HeapFile的readpage会读取一页数据，存放在byte数组
     * 						然后根据读取byte数组返回一个heapPage
     * 	对于每个页获取其被使用的元组即可
     */
    @Test
    public void selectAll() throws IOException, TransactionAbortedException, DbException {
        insert();
        String name="student_table";

        Catalog catalog = Database.getCatalog();
        BufferPool bufferPool = Database.getBufferPool();
        int tableId = catalog.getTableId(name);
        HeapFile heapFile = (HeapFile)catalog.getDatabaseFile(tableId);
        int numPages = heapFile.numPages();
        for (int i = 0; i < numPages; i++) {
            HeapPage page = (HeapPage)bufferPool.getPage(new TransactionId(), new HeapPageId(tableId, i), Permissions.READ_ONLY);//插入时  页id是从0增长的
            Tuple[] tuples = page.getTuples();
            Arrays.stream(tuples)
                    .filter(n -> n != null)
                    .forEach(System.out::println);
        }
    }

    /**
     * 	4.删除一行数据
     * 	此时的插入很简单，是随意的插入
     * 	但删除很困难！！！！
     * 	因为没有组织数据在磁盘中是如何存储的
     * 	每次删除都要遍历一遍，也就是获取所有的元组然后才能得到元组的recordId即位置
     * 	然后才能删除！！！！
     * 	所以，后续的b+树很需要！！！！
     */
        @Test
    public void delete() throws IOException, TransactionAbortedException, DbException {
            //插入小明
            insert();
            System.out.println("删除前--------------");
            //查询一次
            selectAll();

            Catalog catalog = Database.getCatalog();
            BufferPool bufferPool = Database.getBufferPool();
            //准备要删除的元组
            String name="student_table";
            String[] names = {"id", "age", "name"};
            Type[] types = {Type.INT_TYPE, Type.INT_TYPE, Type.STRING_TYPE};
            TupleDesc tupleDesc = new TupleDesc(types,names);
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,new IntField(1));
            tuple.setField(1,new IntField(18));
            tuple.setField(2,new StringField("xiaoming",10));
            //由于不知道位置  需要遍历一遍

            int tableId = catalog.getTableId(name);
            HeapFile heapFile = (HeapFile)catalog.getDatabaseFile(tableId);
            int numPages = heapFile.numPages();
            List<Tuple> collectAll=new ArrayList<>();
            for (int i = 0; i < numPages; i++) {
                HeapPage page = (HeapPage)bufferPool.getPage(new TransactionId(), new HeapPageId(tableId, i), Permissions.READ_ONLY);//插入时  页id是从0增长的
                Tuple[] tuples = page.getTuples();
                List<Tuple> collect = Arrays.stream(tuples)
                        .filter(tupleTemp -> {
                            if(tupleTemp==null){
                                return false;
                            }
                            boolean tupleDesc_equals = tupleTemp.getTupleDesc().equals(tuple.getTupleDesc());
                            Iterator<Field> fields = tupleTemp.fields();
                            int counter=0;
                            while (fields.hasNext()){
                                Field next = fields.next();
                                tupleDesc_equals=next.equals(tuple.getField(counter));
                                counter++;
                            }
                            return tupleDesc_equals;
                        } )
                        .collect(Collectors.toList());

                if (!collect.isEmpty()) collectAll.addAll(collect);
            }

            //找到该需删除的元组
            Tuple tuple1 = collectAll.get(0);
            bufferPool.deleteTuple(new TransactionId(),tuple1);

            System.out.println("删除后------------");
            //删除后在查询一遍
            selectAll();
    }

}
