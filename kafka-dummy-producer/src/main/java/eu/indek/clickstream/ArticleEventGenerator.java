package eu.indek.clickstream;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates ArticleEvents with article IDs ranging from 1 to 10. The probability of generating an article ID
 * is proportional to the article ID itself. For instance, article ID 1 will be generated 1 out of 55 times,
 * article ID 2 will be generated 2 out of 55 times, and so forth.
 */
public class ArticleEventGenerator {
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private static final int MAX_ARTICLE_ID = 10;

    private final List<Integer> list = new ArrayList<>();

    private void fillInList(int n) {
        System.out.println("Filling in list");
        for (int i = 1; i < n + 1; i++) {
            for (int j = 0; j < i; j++) {
                list.add(i);
            }
        }
    }

    private int getRandomElement() {
        if (list.isEmpty()) {
            fillInList(MAX_ARTICLE_ID);
        }

        int randomIndex = (int) (Math.random() * list.size());
        return list.remove(randomIndex);
    }

    public ArticleEvent[] getNextEvents(int n) {
        ArticleEvent[] events = new ArticleEvent[n];
        for (int i = 0; i < n; i++) {
            events[i] = getNextEvent();
        }
        return events;
    }

    public ArticleEvent getNextEvent() {
        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
        return new ArticleEvent("" + getRandomElement(), ArticleEvent.Action.CLICK, now.format(ISO_FORMATTER));
    }
}