// ✅ a) AuthorshipScoreMapper.java
package org.dblp.authorship;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.xml.sax.InputSource;

public class AuthorshipScoreMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            String xml = "<dblp>" + value.toString() + "</dblp>";
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(xml)));
            Element root = (Element) document.getDocumentElement().getFirstChild();

            Map<String, Float> authorScores = AuthorshipScoreStatisticsGenerator.getAuthorScoreMap(root);
            for (Map.Entry<String, Float> entry : authorScores.entrySet()) {
                context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
            }
        } catch (Exception e) {
            // Handle invalid XML
        }
    }
}
