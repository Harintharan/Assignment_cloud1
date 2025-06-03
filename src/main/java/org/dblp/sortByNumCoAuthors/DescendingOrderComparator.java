package org.dblp.sortByNumCoAuthors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingOrderComparator extends WritableComparator {

    public DescendingOrderComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable int1 = (IntWritable) a;
        IntWritable int2 = (IntWritable) b;
        return -1 * int1.compareTo(int2);  // descending
    }
}
