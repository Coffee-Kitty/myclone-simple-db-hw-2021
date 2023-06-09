package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 * 路径结构，表面该行属于PageId某个页面的tupleno行，这PageId所对应的DbPage属于DbFile某个表
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     *页面id
     */
    PageId pid;

    /**
     * 行序号
     */
    int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pid=pid;
        this.tupleno=tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        if(!(o instanceof RecordId)){
            return false;
        }
        RecordId other = (RecordId) o;
        if(other.pid.getTableId()!=this.pid.getTableId()||other.pid.getPageNumber()!=pid.getPageNumber()||other.tupleno!=this.tupleno){
            return false;
        }
        return true;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        String hash = ""+pid.getTableId()+pid.getPageNumber()+tupleno;
        return hash.hashCode();
    }

}
