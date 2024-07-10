package eu.indek.clickstream;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ArticleEventGenerator {
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

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
            fillInList(10);
        }

        int randomIndex = (int) (Math.random() * list.size());
        return list.remove(randomIndex);
    }

    public ArticleEvent getNextEvent() {
        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
        return new ArticleEvent("" + getRandomElement(), ArticleEvent.Action.CLICK, now.format(ISO_FORMATTER));
    }
}