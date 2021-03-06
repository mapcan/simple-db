package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private DbIterator child;
    private Predicate p;
    private TupleDesc td;
    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.child = child;
        this.p = p;
        this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        return p;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        while (child.hasNext()) {
            Tuple t = child.next();
            if (p.filter(t)) {
                return t;
            }
        }
        return null;
    }

    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
