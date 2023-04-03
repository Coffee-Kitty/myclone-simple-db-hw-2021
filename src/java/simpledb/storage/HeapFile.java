package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    /**
     * 文件
     */
    private final File file;

    /**
     * 表头
     */
    private final TupleDesc td;


    /**
     * 实现一个HeapFileIterator
     * 用于迭代该文件 中的一行行元组值
     */
    private static class HeapFileIterator implements DbFileIterator{

        //堆文件
        private final HeapFile heapFile;
        //事务id
        private final TransactionId tid;
        // 元组迭代器
        private Iterator<Tuple> iterator;
        //元组所在页码
        private int whichPage;

        public HeapFileIterator(HeapFile heapFile,TransactionId tid){
            this.heapFile=heapFile;
            this.tid=tid;
        }

        // 获取 当前文件当前页码的页  的迭代器
        private Iterator<Tuple> getPageTuple(int pageNumber) throws TransactionAbortedException, DbException {
            //首先判断页码是否超出文件范围
            if(pageNumber>=0 && pageNumber<heapFile.numPages()){
                HeapPageId heapPageId = new HeapPageId(heapFile.getId(), pageNumber);
                // 从缓存池中查询相应的页面 读权限
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                return page.iterator();
            }

            throw new DbException(String.format("heapFile %d not contain page %d",  heapFile.getId(),pageNumber));
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            //使页码为0
            this.whichPage=0;
            //设置迭代器指向当前文件当前0页码  的页的第一行
            iterator = getPageTuple(whichPage);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            //如果迭代器为null
            if(iterator==null){
                return false;
            }
            //如果当前页码没有元素了  查看下一个页
            if(!iterator.hasNext()){
                //某些页可能没有存储的  所以需要while
                while(whichPage< (heapFile.numPages()-1)){
                    whichPage++;
                    iterator=getPageTuple(whichPage);
                    if(iterator.hasNext()){
                        return true;
                    }
                }
                return false;
            }

            return true;

        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            //如果迭代器为空或者没有下一个元素了  抛出异常
            if(iterator == null || !iterator.hasNext()){
                throw new NoSuchElementException();
            }
            // 返回下一个元组
            return iterator.next();
        }

        //从新开始
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // 清除上一个迭代器
            close();
            // 重新开始
            open();
        }

        @Override
        public void close() {
            iterator=null;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * 由绝对路径生成文件id   将文件id作为表id    所以讲一个文件对应一张表
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        //读取pid此页   此页 此表在file中
        int tableId = pid.getTableId();
        //此页位置  页码pid.pno
        int pageNumber = pid.getPageNumber();
        // 随机访问,指针偏移访问
        RandomAccessFile p=null;
        try{
            //读取当前文件
            //如果当前页码 超出了文件总长度 则抛出异常
            p= new RandomAccessFile(file, "r");
            if((pageNumber+1.0)*BufferPool.getPageSize()>file.length()){
                throw new IllegalArgumentException(String.format("表 %d 页%d 不存在",tableId,pageNumber));
            }

            //准备一个字节数组用于读取页
            //指针f偏移至页码位置  然后读取
            byte[] bytes = new byte[BufferPool.getPageSize()];
            p.seek((long) pageNumber *BufferPool.getPageSize());

            //读取  如果读取的数量少了  说明不存在
            int read = p.read(bytes, 0, BufferPool.getPageSize());//返回读取的数量
            if(read<BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("表%d 页%d 不存在",tableId,pageNumber));
            }
            return new HeapPage(new HeapPageId(tableId,pageNumber),bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //关闭流
            try {
                p.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("表%d 页%d 不存在",tableId,pageNumber));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        //获取该页的页码  查看是否超出文件范围
        int pageNo = page.getId().getPageNumber();
        if(pageNo>numPages()){
            throw new IllegalArgumentException("page is not in the heap file or page id is wrong");
        }
        //然后写
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
        randomAccessFile.seek(pageNo * BufferPool.getPageSize());
        byte[] pageData = page.getPageData();
        randomAccessFile.write(pageData);
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     * 该文件可写的最大页数
     */
    public int numPages() {
        // some code goes here
        // 文件长度 / 每页的字节数
        return (int) Math.floor(file.length()*1.0/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    //返回脏页或者新写的页
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        //所有的 crud操作都需要 经过缓冲区
        //缓冲区中存在 则缓冲区直接返回
        //否则缓冲区去进行读取 然后再返回
        BufferPool bufferPool = Database.getBufferPool();
        int tableid=getId();
        //遍历所有的页 查看是否可写
        for(int i=0;i<numPages();i++){
            HeapPage page = (HeapPage)bufferPool.getPage(tid, new HeapPageId(tableid, i), Permissions.READ_WRITE);
            if(page.getNumEmptySlots() > 0){
                page.insertTuple(t);
                page.markDirty(true,tid);
                list.add(page);
                return list;
            }
        }

        //如果 没有页有空槽 则创建新页
        HeapPage page = new HeapPage(new HeapPageId(tableid, numPages()), HeapPage.createEmptyPageData());
        page.insertTuple(t);
        writePage(page);
        list.add(page);
        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    //返回删除了元组的脏页
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //在缓冲区取得 相应的所在页 然后标记为脏页并删除
        ArrayList<Page> list = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);

        page.markDirty(true,tid);

        list.add(page);
        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

}

