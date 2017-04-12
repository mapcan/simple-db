package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private int intAggr;
    private TreeMap<Integer, Integer> intGbAggr;
    private TreeMap<String, Integer> strGbAggr;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("not support " + what);
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            intAggr = 0;
        } else {
            if (gbfieldtype == Type.INT_TYPE) {
                intGbAggr = new TreeMap<Integer, Integer>();
            } else {
                strGbAggr = new TreeMap<String, Integer>();
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == Aggregator.NO_GROUPING) {
            intAggr += 1;
        } else {
            if (gbfieldtype == Type.INT_TYPE) {
                Integer gbVal = ((IntField)tup.getField(gbfield)).getValue();
                if (!intGbAggr.containsKey(gbVal)) {
                    intGbAggr.put(gbVal, 1);
                } else {
                    intGbAggr.replace(gbVal, intGbAggr.get(gbVal)+1);
                }
            } else {
                String gbVal = ((StringField)tup.getField(gbfield)).getValue();
                if (!strGbAggr.containsKey(gbVal)) {
                    strGbAggr.put(gbVal, 1);
                } else {
                    strGbAggr.replace(gbVal, strGbAggr.get(gbVal)+1);
                }
            }
        }
    }

    private class AggregatorIterator implements DbIterator {
        private boolean isOpen;
        private boolean ngnext;
        private Iterator<Integer> intIter;
        private Iterator<String> strIter;

        public AggregatorIterator() {
            isOpen = false;
        }

        public TupleDesc getTupleDesc() {
            if (gbfield == Aggregator.NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            }
            return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            if (gbfield == Aggregator.NO_GROUPING) {
                ngnext = true;
                return;
            }
            if (gbfieldtype == Type.INT_TYPE) {
                intIter = intGbAggr.keySet().iterator();
            } else {
                strIter = strGbAggr.keySet().iterator();
            }
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new IllegalStateException("Iterator not open");
            }
            if (gbfield == Aggregator.NO_GROUPING) {
                return ngnext;
            }
            if (gbfieldtype == Type.INT_TYPE) {
                return intIter.hasNext();
            }
            return strIter.hasNext();
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                throw new IllegalStateException("Iterator not open");
            }
            if (gbfield == Aggregator.NO_GROUPING) {
                if (ngnext) {
                    Tuple t = new Tuple(getTupleDesc());
                    Field f = new IntField(intAggr);
                    t.setField(0, f);
                    ngnext = false;
                    return t;
                }
                throw new NoSuchElementException();
            }
            if (gbfieldtype == Type.INT_TYPE) {
                int key = intIter.next();
                int value = intGbAggr.get(key);
                Tuple t = new Tuple(getTupleDesc());
                Field f = new IntField(value);
                t.setField(0, new IntField(key));
                t.setField(1, f);
                return t;
            } else {
                String key = strIter.next();
                int value = strGbAggr.get(key);
                Tuple t = new Tuple(getTupleDesc());
                Field f = new IntField(value);
                t.setField(0, new StringField(key, key.length()));
                t.setField(1, f);
                return t;
            }
        }

        public void rewind() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new IllegalStateException("Iterator not open");
            }
            close();
            open();
        }

        public void close() {
            if (!isOpen) {
                throw new IllegalStateException("Iterator not open");
            }
            isOpen = false;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new AggregatorIterator();
    }

}
