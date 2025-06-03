package org.dblp.bucketing;

import org.dblp.utils.Constants;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;

import org.xml.sax.InputSource;

public class BucketingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private String jobName;
    private int bucketSize;

    @Override
    protected void setup(Context context) {
        jobName = context.getConfiguration().get(Constants.JOB_NAME);
        bucketSize = context.getConfiguration().getInt(Constants.BUCKET_SIZE, Integer.parseInt(Constants.DEFAULT_BUCKET_SIZE));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            String xml = "<dblp>" + value.toString() + "</dblp>";
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(xml)));

            Element root = (Element) document.getDocumentElement().getFirstChild();

            if (Constants.BUCKETING_BY_NUM_CO_AUTHOR.equals(jobName)) {
                String tag = root.getTagName();
                String personTag = (tag.equals("book") || tag.equals("proceedings")) ? "editor" : "author";
                int count = root.getElementsByTagName(personTag).getLength();
                String bucket = KeyGenerator.generateKeyByBucket(count, bucketSize);
                context.write(new Text(bucket), new IntWritable(1));
            }

            if (Constants.BUCKETING_BY_PUBLICATION_TYPE.equals(jobName)) {
                String type = root.getTagName();
                context.write(new Text(type), new IntWritable(1));
            }

        } catch (Exception e) {
            // Optionally log/ignore malformed XML
        }
    }
}
