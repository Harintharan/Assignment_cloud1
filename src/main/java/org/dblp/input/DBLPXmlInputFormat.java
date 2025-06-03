package org.dblp.input;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class DBLPXmlInputFormat extends TextInputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new MultiTagXmlRecordReader();
    }
}
