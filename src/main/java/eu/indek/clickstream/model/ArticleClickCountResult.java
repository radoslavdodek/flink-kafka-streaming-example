package eu.indek.clickstream.model;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;

public class ArticleClickCountResult extends ProcessWindowFunction<Long, ArticleClickCount, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Long, ArticleClickCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<ArticleClickCount> out) {
        Long start = context.window().getStart();
        Long end = context.window().getEnd();
        long count = IterableUtils.toStream(elements).mapToLong(Long::longValue).sum();
        out.collect(new ArticleClickCount(s, count, start, end));
    }
}