package simpledb.storage;


import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 页锁 管理类
 */
public class LockManager {

    //key -> 页id
    //value -> 该页 的锁集合
    private final Map<PageId, List<Lock>>  lockMap;

    public LockManager() {
        lockMap=new ConcurrentHashMap<>();
    }

    //是否可以获得某权限锁
    public synchronized boolean acquireLock(PageId pid, TransactionId tid, Permissions permissions){
        //首先查看是否该页有锁
        List<Lock> locks = lockMap.get(pid);
        //如果没有直接加入
        if(locks==null){
            ArrayList<Lock> arrayList = new ArrayList<>();
            Lock lock = new Lock(tid, permissions);
            arrayList.add(lock);
            lockMap.put(pid,arrayList);
            return true;
        }
        //如果已经 只有一把锁
        //如果是 读写锁 则无论permission是什么 都无法获得(除非事务id相同  为同一事务)
        //如果是 只读锁 permission申请只读锁 可以加入  申请读写锁 无法获得（但如果是同一tid可以升级）
        if(locks.size()==1){
            Lock firstLock = locks.get(0);
            if(firstLock.getPermissions().equals(Permissions.READ_WRITE)){

                //如果事务id相同 则为同一事务
                if (firstLock.getTid().equals(tid)){
                    return true;
                }
                return false;
            }else if (firstLock.getPermissions().equals(Permissions.READ_ONLY)){

               if(permissions.equals(Permissions.READ_ONLY)){
                   //事务id相同重复申请只读锁 不能重复加入 直接返回
                   if(tid.equals(firstLock.getTid())){
                       return true;
                   }else{
                       Lock lock = new Lock(tid, Permissions.READ_ONLY);
                       locks.add(lock);
                       return true;
                   }
               }else{
                   //否则 只有事务id相同 方可升级
                    if(tid.equals(firstLock.getTid())){
                        firstLock.setPermissions(Permissions.READ_WRITE);
                        return true;
                    }else {
                        return false;
                    }
               }

            }
        }


        //如果存在多把锁 一定是共享只读锁
        //此时不能申请读写锁
        //申请只读锁 如果事务id相同则直接返回 否则加入locks
        if(locks.size()>1){
            if(permissions.equals(Permissions.READ_WRITE)){
                return false;
            }else {
                for(Lock lock:locks){
                    if(lock.getTid().equals(tid)){
                        return true;
                    }
                }
                Lock lock = new Lock(tid, permissions);
                locks.add(lock);
                return true;
            }
        }

        return false;//此代码不会执行
    }

    //释放锁
    public  synchronized void releaseLock(TransactionId tid,PageId pageId){
        List<Lock> locks = lockMap.get(pageId);
        if(locks==null){
            return;
        }
        for (int i = 0; i < locks.size(); i++) {
            if(locks.get(i).getTid().equals(tid)){
                locks.remove(locks.get(i));
                //注意只有一把锁时的删除情况
                if(locks.size()==0) {
                    lockMap.remove(pageId);
                }
                break;
            }
        }
    }
    //完成事务后释放所有锁
    public void releaseAllLock(TransactionId transactionId){
        Set<PageId> pageIds = lockMap.keySet();
        for(PageId pid:pageIds){
            List<Lock> locks = lockMap.get(pid);
            for (int i = 0; i < locks.size(); i++) {
                if (locks.get(i).getTid().equals(transactionId)){
                    locks.remove(locks.get(i));
                    if(locks.size()==0){
                        lockMap.remove(pid);
                    }
                }
            }
//            if(holdsLock(pid,transactionId)){
//                releaseLock(transactionId,pid);
//            }

        }
    }

    //是否持有锁
    public synchronized boolean holdsLock(PageId pid, TransactionId tid){
        //遍历map 查看对应页上是否有锁的tid 是tid
        List<Lock> locks = lockMap.get(pid);
        if(locks!=null){
            for(Lock lock:locks){
                if(lock.getTid().equals(tid)){
                    return true;
                }
            }
        }
        return false;
    }


}
