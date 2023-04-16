package simpledb.storage;


import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

/**
 * 页锁
 */
public class Lock {

    private TransactionId tid;//事务id
    private Permissions permissions;//权限

    public Lock(TransactionId tid, Permissions permissions) {
        this.tid = tid;
        this.permissions = permissions;
    }

    public TransactionId getTid() {
        return tid;
    }


    public Permissions getPermissions() {
        return permissions;
    }

    public void setPermissions(Permissions permissions) {
        this.permissions = permissions;
    }

    @Override
    public String toString() {
        return "Lock{" +
                "tid=" + tid +
                ", permissions=" + permissions +
                '}';
    }
}
