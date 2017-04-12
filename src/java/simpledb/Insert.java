package simpledb;

import java.util.*;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends Operator {

    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private boolean fetched;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
        this.fetched = false;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
        if (fetched) {
            return null;
        }

        DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        int inserted = 0;
        while (child.hasNext()) {
            try {
                file.insertTuple(tid, child.next());
                inserted += 1;
            } catch (IOException e) {
                throw new DbException("Insert error: " + e);
            }
        }
        Tuple t = new Tuple(getTupleDesc());
        IntField f = new IntField(inserted);
        t.setField(0, f);
        fetched = true;
        return t;
    }
}
