package org.dblp.maxmedian;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.*;
import javax.xml.parsers.*;

import java.io.ByteArrayInputStream;
import java.net.URLDecoder;
import java.util.ArrayList;

public class MaxMedianAvgMapper extends Mapper<LongWritable, Text, Text, MaxMedianAvgWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            String record = value.toString();
            String xml = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><dblp>" + record + "</dblp>";
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setValidating(false);
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new ByteArrayInputStream(xml.getBytes()));

            NodeList authorNodes = doc.getElementsByTagName("author");
            ArrayList<String> authors = new ArrayList<>();

            for (int i = 0; i < authorNodes.getLength(); i++) {
                String name = authorNodes.item(i).getTextContent().trim();
                if (!name.isEmpty()) {
                    authors.add(name);
                }
            }

            int coAuthorCount = authors.size() - 1;

            for (String author : authors) {
                context.write(new Text(author), new MaxMedianAvgWritable(coAuthorCount, coAuthorCount, 1, coAuthorCount));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

