package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.execution.Aggregator;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 *
 * 每次都需要将持久化的文件读到内存当中，作为快速读取的缓冲区
 */
public class BufferPool {
    /** Bytes per page, including header. */
    /**
     * 每页大小 4086byte
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    /**
     * 存储页的最大数量
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * final在字段上面，代表着这个字段不能被重新赋值，但请注意
     * 如果声明时没有赋值，在构造函数里则可以被首次赋值，其它该方法里绝对不行
     */
    /**
     * 当前缓存的页的最大数量
     */
    private final int pageNums;

    /**
     * 页pid的hashcode与缓冲区所存在的页的映射表
     * 存储的页面
     */
    //pageStore（即方法）都是允许调用的。只是不能再将这个pageStore变量指向其他的实例化对象了，即不能再出现pageStore= new ConcurrentHashMap<PageId,Page>(); 的代码。
    //private final ConcurrentHashMap<Integer, Page> pageStore;
    //重构了
    private final  LRUCache<Integer,Page> pageStore;
    /**
     * 缓冲区的基于页的锁管理
     */
    private LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pageNums=numPages;
        this.pageStore=new LRUCache<>(numPages);
        lockManager=new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        //首先只有获取锁 才能跳出死循环
        //死循环有时间限制
        boolean isAcquire = lockManager.acquireLock(pid,tid,perm);
        long startTime = System.currentTimeMillis();
        while (!isAcquire){
            long now = System.currentTimeMillis();
            if(now-startTime > 500){
                throw new TransactionAbortedException();
            }
            isAcquire = lockManager.acquireLock(pid,tid,perm);
        }
        //1.从LRU缓存中得到
        Page page = pageStore.get(pid.hashCode());
        if(page==null){
            //如果不存在 从磁盘中取
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            if (page!=null){

                //取完放到LRU缓存中
                int hashCode = page.getId().hashCode();
                if(!pageStore.getCache().containsKey(hashCode)&&pageStore.getCache().size()>=pageNums){
                    evictPage();
                }
                pageStore.put(hashCode,page);

            }else{
                throw new DbException("database have not the page");
            }
        }
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */

    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);

    }


    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    //事务提交  如果成功 刷盘
    //         如果失败 回滚
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit){
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            //回滚
            rollbackTransaction(tid);
        }
        //完成事务或者中止事务后释放所有的锁
          lockManager.releaseAllLock(tid);

    }
    //回滚页面
    private  void rollbackTransaction(TransactionId tid){
            List<Page> allV = pageStore.getAllV();
            Iterator<Page> iterator = allV.iterator();
            while (iterator.hasNext()){
                Page next = iterator.next();
                if(next.isDirty()!=null&&next.isDirty().equals(tid)){
                    //如果事务id相同
                    //则重新读取页到缓存区即可
                    DbFile databaseFile = Database.getCatalog().getDatabaseFile(next.getId().getTableId());
                    Page page = databaseFile.readPage(next.getId());
                    pageStore.put(next.getId().hashCode(),page);//put后需调整到队列头
                }
            }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        //1.调用DBFile DBFile如果发现LRU缓存中有   则调用Page 在Page上插入，并返回插入的脏页
        //                   否则其直接写，  那么需要将新页添加到缓存
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        //2.无论是返回的脏页还是新页  都需put到LRU缓存
        for(Page page:pages){

            int hashCode = page.getId().hashCode();
            if(!pageStore.getCache().containsKey(hashCode)&&pageStore.getCache().size()>=pageNums){
                evictPage();
            }
            pageStore.put(hashCode,page);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        //1.还是先调用DBFile  DBFile从缓存中拿页Page 如果有则删除元组并返回已经标记了的脏页
        //                                       否则LRU缓存 pageStore会调用DBFile从磁盘读页 然后加入LRU缓存   然后再删除元组并返回脏页
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        //？？？ 由于对象都是引用  那么pageStore已经全是脏页了  被修改过了
        for(Page page:pages){
//            page.markDirty(true,tid);
            int hashCode = page.getId().hashCode();
            if(!pageStore.getCache().containsKey(hashCode)&&pageStore.getCache().size()>=pageNums){
                evictPage();
            }
            pageStore.put(hashCode,page);
        }


    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
   // public synchronized void flushAllPages() throws IOException {
    public  void flushAllPages() throws IOException {
        for(Page page:pageStore.getAllV()){
            PageId id = page.getId();
            flushPage(id);//flushPage会自动判断脏页
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pageStore.removeK(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    //将页刷新到磁盘
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = pageStore.get(pid.hashCode());
        //磁盘不存在 此页或则 此页不为脏页无需写入
        if(page==null||page.isDirty()==null){
            return;
        }
        //否则写进磁盘文件
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        //移除脏页和事务标签
        page.markDirty(false,null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    //事务提交后成功刷盘
    public  synchronized void flushPages(TransactionId tid) throws IOException {
        for(Page page:pageStore.getAllV()){
            if(page.isDirty()==tid){
                flushPage(page.getId());
            }
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        //有了事务后   只有事务提交了 并且是脏页 才能discard
        List<Page> allV = pageStore.getAllV();
        //获取逆序的LRU
        for (int i = allV.size()-1; i >=0 ; i--) {
            TransactionId dirty = allV.get(i).isDirty();//返回是否为脏页 如果为脏页返回持有事务id 否则返回null
            if(dirty==null){
                PageId id = allV.get(i).getId();
                try {
                    flushPage(id);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                pageStore.removeK(id.hashCode());
                return;
            }
        }
        //查看是否真的evict了
        throw new DbException("All Page Are Dirty Page");

    }

}
