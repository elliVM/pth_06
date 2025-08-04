package com.teragrep.pth_06.ast;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;

public class RowKeyExpressionImpl implements RowKeyExpression {
    public final Long streamId;
    public final Long earliest;
    public final Long latest;

    public RowKeyExpressionImpl(Long streamId, Long earliest, Long latest) {
        this.streamId = streamId;
        this.earliest = earliest;
        this.latest = latest;
    }

    @Override
    public byte[] rowKeyBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(3 * 8);
        buffer.put(Bytes.toBytes(streamId));
        buffer.put(Bytes.toBytes(earliest));
        buffer.put(Bytes.toBytes(latest));
        return buffer.array();
    }
}
