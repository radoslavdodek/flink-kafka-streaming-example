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
    public Long eventTime;

    public ArticleEvent() {
    }

    public ArticleEvent(String articleId, Action action, Long eventTime) {
        this.articleId = articleId;
        this.action = action;
        this.eventTime = eventTime;
    }

    public String getArticleId() {
        return articleId;
    }

    public Action getAction() {
        return action;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public static ArticleEvent fromJsonString(String jsonString) {
        try {
            return JSON.readValue(jsonString, ArticleEvent.class);
        } catch (IOException e) {
            return null;
        }
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
