package eu.indek.clickstream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ArticleEvent {

    private final static ObjectMapper JSON = new ObjectMapper();

    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public enum Action {
        CLICK
    }

    public String articleId;
    public Action action;
    public String eventTime;

    public ArticleEvent(String articleId, Action action, String eventTime) {
        this.articleId = articleId;
        this.action = action;
        this.eventTime = eventTime;
    }

    public String toJsonString() {
        try {
            return JSON.writeValueAsString(this);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format("ArticleId: %s, Action: %s, EventTime: %s", articleId, action, eventTime);
    }

}
