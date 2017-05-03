package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId tid;
    private DbIterator child;
    private boolean fetched;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (fetched) {
            return null;
        }
        int deleted = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            RecordId rid = t.getRecordId();
            PageId pid = rid.getPageId();
            int tableid = pid.getTableId();
            DbFile file = Database.getCatalog().getDatabaseFile(tableid);
            file.deleteTuple(tid, t);
            deleted += 1;
        }
        Tuple t = new Tuple(getTupleDesc());
        Field f = new IntField(deleted);
        t.setField(0, f);
        fetched = true;
        return t;
    }

    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
