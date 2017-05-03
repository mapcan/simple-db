package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int width;
    private int tuples;

    private ArrayList<Bucket> bucketList;

    private class Bucket {
        private int left;
        private int right;
        private int width;
        private int height;
        private int index;

        public Bucket(int left, int right, int width, int index) {
            this.left = left;
            this.right = right;
            this.width = width;
            this.index = index;
            this.height = 0;
        }

        public int getIndex() {
            return index;
        }

        public int getLeft() {
            return left;
        }

        public int getRight() {
            return right;
        }

        public int getHeight() {
            return height;
        }

        public void incHeight() {
            height += 1;
        }

        public void decHeight() {
            height -= 1;
        }
    }

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.tuples = 0;
        this.width = (max - min + 1) / buckets;
        if (width == 0) {
            width = 1;
        } else if ((max - min + 1) % buckets != 0) {
            buckets += 1;
        }
        this.bucketList = new ArrayList<Bucket>(buckets);
        for (int i = 0; i < buckets; i++) {
            int left = min + i * width;
            int right = left + width - 1;
            Bucket bucket = new Bucket(left, right, width, i);
            bucketList.add(i, bucket);
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        tuples += 1;
        Bucket bucket = getBucketForValue(v);
        if (bucket == null)
            return;
        bucket.incHeight();
    }

    private Bucket getBucketForValue(int v) {
        int i = (v - min) / width;
        try {
            return bucketList.get(i);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    private double estimateEqualSelectivity(int v) {
        Bucket bucket = getBucketForValue(v);
        if (bucket == null) {
            return 0.0;
        }
        return (double)bucket.getHeight() / width / tuples;
    }

    private double estimateGreaterThanSelectivity(int v) {
        double selectivity = 0.0;
        if (v < min) {
            return 1.0;
        }
        if (v >= max) {
            return 0.0;
        }
        Bucket bucket = getBucketForValue(v);
        double b_f = ((double)bucket.getRight() - v) / tuples;
        double b_part = (double)bucket.getHeight() / width;
        double b_contrib = b_f * b_part;
        selectivity += b_contrib;
        for (int b = bucket.getIndex() + 1; b < buckets; b++) {
            bucket = bucketList.get(b);
            b_f = (double)bucket.getHeight() / tuples;
            selectivity += b_f;
        }
        return selectivity;
    }

    private double estimateLessThanSelectivity(int v) {
        double selectivity = 0.0;
        if (v <= min) {
            return 0.0;
        }
        if (v > max) {
            return 1.0;
        }
        Bucket bucket = getBucketForValue(v);
        double b_f = ((double)v - bucket.getLeft()) / tuples;
        double b_part = (double)bucket.getHeight() / width;
        double b_contrib = b_f * b_part;
        selectivity += b_contrib;
        int ul = bucket.getIndex();
        for (int b = 0; b < ul; b++) {
            bucket = bucketList.get(b);
            b_f = (double)bucket.getHeight() / tuples;
            selectivity += b_f;
        }
        return selectivity;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (op == Predicate.Op.EQUALS) {
            return estimateEqualSelectivity(v);
        } else if (op == Predicate.Op.NOT_EQUALS) {
            return 1.0 - estimateEqualSelectivity(v);
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            return estimateGreaterThanSelectivity(v - 1);
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            return estimateLessThanSelectivity(v + 1);
        } else if (op == Predicate.Op.GREATER_THAN) {
            return estimateGreaterThanSelectivity(v);
        } else if (op == Predicate.Op.LESS_THAN) {
            return estimateLessThanSelectivity(v);
        } else {
            return 0.0;
        }
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String s = "";
        for (Bucket bucket: bucketList) {
            s += "Bucket ";
            s += bucket.getIndex();
            s += " ";
            s += "Height ";
            s += bucket.getHeight();
            s += "\n";
        }
        return s;
    }
}
