package com.teragrep.pth_06.planner;

import com.teragrep.pth_06.ast.ScanRange;
import com.teragrep.pth_06.config.Config;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public final class EpochHourResult {
    private final Table logfileTable;
    private final Long startHour;
    private final Long endHour;
    private final List<ScanRange> ranges;
    private final Config config;
    private final List<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> records;

    public EpochHourResult(Table logfileTable, Long startHour, List<ScanRange> ranges, Config config) {
        this(logfileTable, startHour, startHour + config.batchConfig.quantumLength, ranges, config, new ArrayList<>());
    }

    private EpochHourResult(Table logfileTable, Long startHour, Long endHour, List<ScanRange> ranges, Config config, List<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> records) {
        this.logfileTable = logfileTable;
        this.startHour = startHour;
        this.endHour = endHour;
        this.ranges = ranges;
        this.config = config;
        this.records = records;
    }

    public Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> toJooqResults() {
        fillCacheIfEmpty();
        final Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> results;
        final DSLContext using = DSL.using(SQLDialect.MYSQL);
        final Field<ULong> idField = DSL.field("id", ULong.class);
        final Field<String> directoryField = DSL.field("directory", String.class);
        final Field<String> streamField = DSL.field("stream", String.class);
        final Field<String> hostField = DSL.field("host", String.class);
        final Field<String> logtagField = DSL.field("logtag", String.class);
        final Field<Date> logdateField = DSL.field("logdate", Date.class);
        final Field<String> bucketField = DSL.field("bucket", String.class);
        final Field<String> pathField = DSL.field("path", String.class);
        final Field<Long> logtimeField = DSL.field("logtime", Long.class);
        final Field<ULong> filesizeField = DSL.field("filesize", ULong.class);
        final Field<ULong> uncompressedFilesizeField = DSL.field("uncompressed_filesize", ULong.class);
        results = using.newResult(idField, directoryField, streamField, hostField, logtagField, logdateField, bucketField, pathField, logtimeField, filesizeField, uncompressedFilesizeField);
        results.addAll(records);
        return results;
    }

    public WeightedOffset offset() {
        fillCacheIfEmpty();
        WeightedOffset offset;
        if (!records.isEmpty()) {
            Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> record = records.get(0);
            long totalSize = record.value1().longValue();
            offset = new WeightedOffset(endHour, totalSize);
        } else {
            offset = new WeightedOffset();
        }
        return offset;
}

public Long earliest() {
    return startHour;
}

public Long latest() {
    return endHour;
}

private void fillCacheIfEmpty() {
    if (records.isEmpty()) {
        // todo get cf name from config
        for (ScanRange range : ranges) {
            Scan scan = range.toScan();
            try (ResultScanner scanner = logfileTable.getScanner(scan)) {
                for (org.apache.hadoop.hbase.client.Result hbaseRow : scanner) {
                    records.add(record11FromHbaseResult(hbaseRow));

                }
            } catch (IOException e) {

            }

        }
    }
}

private Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> record11FromHbaseResult(org.apache.hadoop.hbase.client.Result result) {
    byte[] columnFamilyBytes = Bytes.toBytes("meta");
    ULong id = ULong.valueOf(Bytes.toLong(result.getValue(columnFamilyBytes, Bytes.toBytes("i"))));
    String directory = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("d")));
    String stream = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("s")));
    String host = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("h")));
    String logtag = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("lt")));
    Date logdate = Date.valueOf(Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("ld"))));
    String bucket = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("b")));
    String path = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("p")));
    Long logtime = Bytes.toLong(result.getValue(columnFamilyBytes, Bytes.toBytes("t")));
    ULong filesize = ULong.valueOf(Bytes.toLong(result.getValue(columnFamilyBytes, Bytes.toBytes("fs"))));
    ULong uncompressedFileSize = ULong.valueOf(Bytes.toLong(result.getValue(columnFamilyBytes, Bytes.toBytes("ufs"))));

    DSLContext using = DSL.using(SQLDialect.MYSQL);

    Field<ULong> idField = DSL.field("id", ULong.class);
    Field<String> directoryField = DSL.field("directory", String.class);
    Field<String> streamField = DSL.field("stream", String.class);
    Field<String> hostField = DSL.field("host", String.class);
    Field<String> logtagField = DSL.field("logtag", String.class);
    Field<Date> logdateField = DSL.field("logdate", Date.class);
    Field<String> bucketField = DSL.field("bucket", String.class);
    Field<String> pathField = DSL.field("path", String.class);
    Field<Long> logtimeField = DSL.field("logtime", Long.class);
    Field<ULong> filesizeField = DSL.field("filesize", ULong.class);
    Field<ULong> uncompressedFilesizeField = DSL.field("uncompressed_filesize", ULong.class);

    Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> newRecord11 = using.newRecord(idField, directoryField, streamField, hostField, logtagField, logdateField, bucketField, pathField, logtimeField, filesizeField, uncompressedFilesizeField);
    newRecord11.set(idField, id);
    newRecord11.set(directoryField, directory);
    newRecord11.set(streamField, stream);
    newRecord11.set(hostField, host);
    newRecord11.set(logtagField, logtag);
    newRecord11.set(logdateField, logdate);
    newRecord11.set(bucketField, bucket);
    newRecord11.set(pathField, path);
    newRecord11.set(logtimeField, logtime);
    newRecord11.set(filesizeField, filesize);
    newRecord11.set(uncompressedFilesizeField, uncompressedFileSize);

    return newRecord11;
}
}
