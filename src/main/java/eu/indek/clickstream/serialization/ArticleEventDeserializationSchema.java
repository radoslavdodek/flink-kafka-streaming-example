package eu.indek.clickstream.serialization;

import eu.indek.clickstream.ArticleEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class ArticleEventDeserializationSchema implements DeserializationSchema<ArticleEvent> {
    @Override
    public ArticleEvent deserialize(byte[] message) {
        return ArticleEvent.fromJsonString(new String(message));
    }

    @Override
    public boolean isEndOfStream(ArticleEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ArticleEvent> getProducedType() {
        return Types.POJO(ArticleEvent.class);
    }
}