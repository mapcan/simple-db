package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ArrayList<Integer> intAggr;
    private TreeMap<Integer, ArrayList<Integer>> intGbAggr;
    private TreeMap<String, ArrayList<Integer>> strGbAggr;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            intAggr = new ArrayList<Integer>();
        } else {
            if (gbfieldtype == Type.INT_TYPE) {
                intGbAggr = new TreeMap<Integer, ArrayList<Integer>>();
            } else {
                strGbAggr = new TreeMap<String, ArrayList<Integer>>();
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == Aggregator.NO_GROUPING) {
            Field field = tup.getField(afield);
            intAggr.add(((IntField)field).getValue());
        } else {
            if (gbfieldtype == Type.INT_TYPE) {
                Integer gbVal = ((IntField)tup.getField(gbfield)).getValue();
                Integer aggrVal = ((IntField)tup.getField(afield)).getValue();
                if (!intGbAggr.containsKey(gbVal)) {
                    intGbAggr.put(gbVal, new ArrayList<Integer>());
                }
                intGbAggr.get(gbVal).add(aggrVal);
            } else {
                String gbVal = ((StringField)tup.getField(gbfield)).getValue();
                Integer aggrVal = ((IntField)tup.getField(afield)).getValue();
                if (!strGbAggr.containsKey(gbVal)) {
                    strGbAggr.put(gbVal, new ArrayList<Integer>());
                }
                strGbAggr.get(gbVal).add(aggrVal);
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

        private int sum(ArrayList<Integer> arr) {
            int s = 0;
            for (int i : arr) {
                s += i;
            }
            return s;
        }

        private int evalAggr(ArrayList<Integer> arr) {
            int result = 0;
            switch (what) {
                case MIN:
                    result = Collections.min(arr);
                    break;
                case MAX:
                    result = Collections.max(arr);
                    break;
                case SUM:
                    result = sum(arr);
                    break;
                case AVG:
                    if (arr.isEmpty()) {
                        result = 0;
                    }
                    result = sum(arr) / arr.size();
                    break;
                case COUNT:
                    result = arr.size();
                    break;
            }
            return result;
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
                    int result = evalAggr(intAggr);
                    Tuple t = new Tuple(getTupleDesc());
                    Field f = new IntField(result);
                    t.setField(0, f);
                    ngnext = false;
                    return t;
                }
                throw new NoSuchElementException();
            }
            if (gbfieldtype == Type.INT_TYPE) {
                int key = intIter.next();
                int result = evalAggr(intGbAggr.get(key));
                Tuple t = new Tuple(getTupleDesc());
                Field f = new IntField(result);
                t.setField(0, new IntField(key));
                t.setField(1, f);
                return t;
            } else {
                String key = strIter.next();
                int result = evalAggr(strGbAggr.get(key));
                Tuple t = new Tuple(getTupleDesc());
                Field f = new IntField(result);
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
