package org.dblp.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MultiTagXmlRecordReader extends RecordReader<LongWritable, Text> {

    private FSDataInputStream fsin;
    private long start;
    private long end;
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    private byte[][] startTags;
    private byte[][] endTags;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = context.getConfiguration();
        Path path = split.getPath();
        fsin = path.getFileSystem(conf).open(path);

        this.start = split.getStart();
        this.end = start + split.getLength();
        fsin.seek(start);

        startTags = convertToByteArrays(conf.getStrings("possible-start-tags"));
        endTags = convertToByteArrays(conf.getStrings("possible-end-tags"));
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        byte[] startTag = readUntilMatch(startTags, false);

        if (fsin.getPos() >= end || startTag == null) {
            return false;
        }

        buffer.reset();
        buffer.write(startTag);

        byte[] endTag = readUntilMatch(endTags, true);

        if (endTag != null) {
            key.set(fsin.getPos());
            value.set(buffer.toByteArray());
            return true;
        }

        return false;
    }

    private byte[][] convertToByteArrays(String[] tags) {
        byte[][] result = new byte[tags.length][];
        for (int i = 0; i < tags.length; i++) {
            result[i] = tags[i].getBytes(StandardCharsets.UTF_8);
        }
        return result;
    }

    private byte[] readUntilMatch(byte[][] tags, boolean writeToBuffer) throws IOException {
        int[] matchPos = new int[tags.length];

        while (true) {
            int b = fsin.read();
            if (b == -1) return null;

            if (writeToBuffer) buffer.write(b);

            for (int i = 0; i < tags.length; i++) {
                if (b == tags[i][matchPos[i]]) {
                    matchPos[i]++;
                    if (matchPos[i] >= tags[i].length) return tags[i];
                } else {
                    matchPos[i] = 0;
                }
            }

            if (!writeToBuffer && fsin.getPos() >= end) return null;
        }
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return (fsin.getPos() - start) / (float)(end - start);
    }

    @Override
    public void close() throws IOException {
        fsin.close();
    }
}
