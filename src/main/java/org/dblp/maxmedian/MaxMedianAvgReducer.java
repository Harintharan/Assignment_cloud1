package org.dblp.maxmedian;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class MaxMedianAvgReducer extends Reducer<Text, MaxMedianAvgWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<MaxMedianAvgWritable> values, Context context)
            throws IOException, InterruptedException {

        float sum = 0;
        long count = 0;
        float max = 0;
        ArrayList<Long> medians = new ArrayList<>();

        for (MaxMedianAvgWritable val : values) {
            sum += val.getAvg();
            count += val.getCount();
            max = Math.max(max, val.getMax());
            medians.add(val.getMedian());
        }

        Collections.sort(medians);
        long median = medians.get((medians.size() - 1) / 2);
        float avg = sum / count;

        context.write(key, new Text(";" + max + ";" + median + ";" + avg));
    }
}
