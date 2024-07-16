package eu.indek.clickstream.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ArticleClickCount {
    private final static ObjectMapper JSON = new ObjectMapper();

    private final String articleId;
    private final Long count;
    private final Long windowStart;
    private final Long windowEnd;

    public ArticleClickCount(String articleId, Long count, Long windowStart, Long windowEnd) {
        this.articleId = articleId;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public String getArticleId() {
        return articleId;
    }

    public Long getCount() {
        return count;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
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
        return "ArticleClickCount {" +
                "article_id='" + articleId + '\'' +
                ", count=" + count +
                ", window_start=" + windowStart +
                ", window_end=" + windowEnd +
                '}';
    }
}
