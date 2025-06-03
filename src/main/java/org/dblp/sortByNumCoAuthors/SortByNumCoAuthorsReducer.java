package org.dblp.sortByNumCoAuthors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class SortByNumCoAuthorsReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueCoAuthors = new HashSet<>();
        for (Text value : values) {
            uniqueCoAuthors.add(value.toString());
        }
        context.write(key, new IntWritable(uniqueCoAuthors.size()));
    }
}
