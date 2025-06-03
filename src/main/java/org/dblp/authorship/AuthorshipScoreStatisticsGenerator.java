// ✅ c) AuthorshipScoreStatisticsGenerator.java
package org.dblp.authorship;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AuthorshipScoreStatisticsGenerator {

    public static Map<String, Float> getAuthorScoreMap(Element publicationElement) {
        final float ONE_BY_FOUR = 0.25f;
        final float ONE = 1f;

        String tag = publicationElement.getTagName();
        String authorTag = tag.equals("book") || tag.equals("proceedings") ? "editor" : "author";

        NodeList authorsNodes = publicationElement.getElementsByTagName(authorTag);
        int size = authorsNodes.getLength();

        Map<String, Float> scoreMap = new HashMap<>();
        ArrayList<String> authors = new ArrayList<>();

        if (size > 0) {
            float score = ONE / size;
            for (int i = 0; i < size; i++) {
                String author = authorsNodes.item(i).getTextContent();
                authors.add(author);
                scoreMap.put(author, score);
            }

            for (int i = size - 1; i > 0; i--) {
                String currAuthor = authors.get(i);
                String prevAuthor = authors.get(i - 1);
                float diff = ONE_BY_FOUR * scoreMap.get(currAuthor);
                scoreMap.put(currAuthor, scoreMap.get(currAuthor) - diff);
                scoreMap.put(prevAuthor, scoreMap.get(prevAuthor) + diff);
            }
        }
        return scoreMap;
    }
}