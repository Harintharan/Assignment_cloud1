// ✅ b) AuthorshipScoreReducer.java
package org.dblp.authorship;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorshipScoreReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws java.io.IOException, InterruptedException {
        float sum = 0f;
        for (FloatWritable val : values) {
            sum += val.get();
        }
        context.write(key, new FloatWritable(sum));
    }
}