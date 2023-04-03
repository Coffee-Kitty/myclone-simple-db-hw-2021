package simpledb.storage;

import simpledb.execution.OpIterator;

import java.util.*;

/**
 * Implements a OpIterator by wrapping an Iterable<Tuple>.
 */
//元组的迭代器
public class TupleIterator implements OpIterator {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    private Iterator<Tuple> i = null;
    private TupleDesc td = null;
    //所有实现了 iterable接口的对象
    //例如  ArrayList等
    private Iterable<Tuple> tuples = null;

    /**
     * Constructs an iterator from the specified Iterable, and the specified
     * descriptor.
     * 
     * @param tuples
     *            The set of tuples to iterate over
     */
    public TupleIterator(TupleDesc td, Iterable<Tuple> tuples) {
        this.td = td;
        this.tuples = tuples;

        // check that all tuples are the right TupleDesc
        for (Tuple t : tuples) {
            if (!t.getTupleDesc().equals(td))
                throw new IllegalArgumentException(
                        "incompatible tuple in tuple set");
        }
    }

    public void open() {
        i = tuples.iterator();
    }

    public boolean hasNext() {
        return i.hasNext();
    }

    public Tuple next() {
        return i.next();
    }

    public void rewind() {
        close();
        open();
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void close() {
        i = null;
    }
}
