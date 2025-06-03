package org.dblp.sortByNumCoAuthors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.HashSet;

public class SortByNumCoAuthorsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            String xml = "<?xml version=\"1.0\"?><dblp>" + value.toString() + "</dblp>";

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setValidating(false);
            factory.setNamespaceAware(false);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));

            NodeList authors = doc.getElementsByTagName("author");

            for (int i = 0; i < authors.getLength(); i++) {
                String author = authors.item(i).getTextContent().trim();
                for (int j = 0; j < authors.getLength(); j++) {
                    if (i != j) {
                        String coAuthor = authors.item(j).getTextContent().trim();
                        context.write(new Text(author), new Text(coAuthor));
                    }
                }
            }
        } catch (Exception e) {
            // Handle parsing issues safely
            e.printStackTrace();
        }
    }
}
