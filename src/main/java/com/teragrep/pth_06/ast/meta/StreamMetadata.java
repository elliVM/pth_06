package com.teragrep.pth_06.ast.meta;

import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record6;
import org.jooq.Result;
import org.jooq.types.UInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.stream.Collectors;

import static com.teragrep.pth_06.jooq.generated.streamdb.Streamdb.STREAMDB;

/**
 * List all indexes in the SQL metadata
 */
public final class StreamMetadata {
    private final Logger LOGGER = LoggerFactory.getLogger(StreamMetadata.class);
    private final DSLContext ctx;
    private final Condition condition;

    public StreamMetadata(final DSLContext ctx, Condition condition) {
        this.ctx = ctx;
        this.condition = condition;
    }

    public List<Long> streamIdList() {
        Result<Record1<UInteger>> streamIdResult = ctx.select(
                        STREAMDB.STREAM.ID
                )
                .from(STREAMDB.STREAM)
                .join(STREAMDB.LOG_GROUP).on(STREAMDB.STREAM.GID.eq(STREAMDB.LOG_GROUP.ID))
                .join(STREAMDB.HOST).on(STREAMDB.LOG_GROUP.ID.eq(STREAMDB.HOST.GID))
                .where(condition)
                .fetch();
        LOGGER.info("fetched <{}>", streamIdResult);
        List<Long> streamIdList = streamIdResult.getValues(STREAMDB.STREAM.ID, UInteger.class)
                .stream().map(r -> r.longValue())
                .collect(Collectors.toList());
        LOGGER.info("fetched <{}> stream id(s)", streamIdList.size());
        return streamIdList;
    }
}
