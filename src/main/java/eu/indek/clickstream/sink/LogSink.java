package eu.indek.clickstream.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogSink<T> implements Sink<T> {
    private final String logFormat;

    public LogSink(String logFormat) {
        this.logFormat = logFormat;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) {
        return new LogSinkWriter<>(logFormat);
    }

    private static class LogSinkWriter<T> implements SinkWriter<T> {
        private final static Logger logger = LogManager.getLogger(LogSink.class);
        private final String logFormat;

        public LogSinkWriter(String logFormat) {
            this.logFormat = logFormat;
        }

        @Override
        public void write(T element, Context context) {
            logger.info(logFormat, () -> element);
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public void close() {
        }
    }
}