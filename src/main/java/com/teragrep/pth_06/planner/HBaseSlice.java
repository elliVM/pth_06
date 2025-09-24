/*
 * Teragrep Archive Datasource (pth_06)
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_06.planner;

import com.teragrep.pth_06.ast.ScanRange;
import com.teragrep.pth_06.config.Config;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Slice of a quantum length from hbase results, stop offset is exclusive
 */
public final class HBaseSlice implements Slice {

    private final Logger LOGGER = LoggerFactory.getLogger(HBaseSlice.class);

    private final Table logfileTable;
    private final Long startOffset;
    private final Long stopOffset;
    private final List<ScanRange> ranges;
    private final Config config;
    private final List<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> records;

    public HBaseSlice(Table logfileTable, Long startOffset, Long stopOffset, List<ScanRange> ranges, Config config) {
        this(
                logfileTable,
                startOffset,
                stopOffset,
                ranges,
                config,
                new ArrayList<>()
        );
    }

    private HBaseSlice(
            Table logfileTable,
            Long startOffset,
            Long stopOffset,
            List<ScanRange> ranges,
            Config config,
            List<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> records
    ) {
        this.logfileTable = logfileTable;
        this.startOffset = startOffset;
        this.stopOffset = stopOffset;
        this.ranges = ranges;
        this.config = config;
        this.records = records;
    }

    @Override
    public Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> asResult() {
        scanToMemory();
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
        results = using
                .newResult(
                        idField, directoryField, streamField, hostField, logtagField, logdateField, bucketField,
                        pathField, logtimeField, filesizeField, uncompressedFilesizeField
                );
        results.addAll(records);
        LOGGER.info("combined <{}> rows into Result sized <{}>", records.size(), results.size());
        return results;
    }

    @Override
    public WeightedOffset weightedOffset() {
        scanToMemory();
        final WeightedOffset offset;
        if (!records.isEmpty()) {
            final Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> record = records
                    .get(0);
            long fileSize = record.value10().longValue();
            offset = new WeightedOffset(stopOffset, fileSize);
        } else {
            offset = new WeightedOffset();
        }
        return offset;
    }

    @Override
    public long start() {
        return startOffset;
    }

    @Override
    public long stop() {
        return stopOffset;
    }

    private void scanToMemory() {
        if (records.isEmpty()) {
            for (ScanRange range : ranges) {
                ScanRange rangeBetween = range.toRangeBetween(startOffset, stopOffset);
                if (rangeBetween.isStub()) { // skip if range did not intersect
                    LOGGER.info("Skipping range <{}>", range);
                    continue;
                }
                LOGGER.info("Scanning with scan range <{}>", rangeBetween);
                try (final ResultScanner scanner = logfileTable.getScanner(rangeBetween.toScan())) {
                    long rowCount = 0;
                    for (final org.apache.hadoop.hbase.client.Result result : scanner) {
                        byte[] rowKeyBytes = result.getRow();
                        ByteBuffer buffer = ByteBuffer.wrap(rowKeyBytes);
                        LOGGER.info("Result with row key values stream_id <{}>, epoch <{}>", buffer.getLong(), buffer.getLong());
                        records.add(record11FromHbaseResult(result));
                        rowCount++;
                    }
                    LOGGER.info("ScanRange <{}> had <{}> results", rangeBetween, rowCount);

                } catch (IOException e) {
                    throw new RuntimeException("Error reading HBase result for slice: " + e.getMessage());
                }
            }
        }
    }

    // TODO remove when sure not needed
    private Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> emptyRecord() {
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

        Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> newRecord11 = using
                .newRecord(
                        idField, directoryField, streamField, hostField, logtagField, logdateField, bucketField,
                        pathField, logtimeField, filesizeField, uncompressedFilesizeField
                );

        newRecord11.set(idField, ULong.valueOf(0L));
        newRecord11.set(directoryField, "");
        newRecord11.set(streamField, "");
        newRecord11.set(hostField, "");
        newRecord11.set(logtagField, "");
        newRecord11.set(logdateField, Date.valueOf("1970-01-01"));
        newRecord11.set(bucketField, "");
        newRecord11.set(pathField, "");
        newRecord11.set(logtimeField, 0L);
        newRecord11.set(filesizeField, ULong.valueOf(0L));
        newRecord11.set(uncompressedFilesizeField, ULong.valueOf(0L));

        return newRecord11;
    }

    private Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> record11FromHbaseResult(
            org.apache.hadoop.hbase.client.Result result
    ) {
        byte[] columnFamilyBytes = Bytes.toBytes("meta");
        ULong id = ULong.valueOf(Bytes.toLong(result.getValue(columnFamilyBytes, Bytes.toBytes("i"))));
        String directory = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("d")));
        String stream = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("s")));
        String host = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("h")));
        String logtag = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("lt")));
        String dateString = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("ld")));
        Date logdate = Date.valueOf(dateString);
        String bucket = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("b")));
        String path = Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes("p")));
        Long logtime = Bytes.toLong(result.getValue(columnFamilyBytes, Bytes.toBytes("t")));
        ULong filesize = ULong.valueOf(Bytes.toLong(result.getValue(columnFamilyBytes, Bytes.toBytes("fs"))));

        ULong uncompressedFileSize;
        byte[] uncompressedFileSizeBytes = result.getValue(columnFamilyBytes, Bytes.toBytes("ufs"));

        // currently encountered null values are replicated as empty byte arrays to hbase
        if (uncompressedFileSizeBytes.length == 0) {
            // null value used only to pass it to the generated record representation of the hbase results
            uncompressedFileSize = null;
        } else {
            uncompressedFileSize = ULong.valueOf(Bytes.toLong(uncompressedFileSizeBytes));
        }

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

        Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> newRecord11 = using
                .newRecord(
                        idField, directoryField, streamField, hostField, logtagField, logdateField, bucketField,
                        pathField, logtimeField, filesizeField, uncompressedFilesizeField
                );
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

    @Override
    public boolean equals(final Object object) {
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final HBaseSlice hBaseSlice = (HBaseSlice) object;
        return Objects.equals(logfileTable, hBaseSlice.logfileTable) && Objects
                .equals(startOffset, hBaseSlice.startOffset) && Objects.equals(ranges, hBaseSlice.ranges)
                && Objects.equals(config, hBaseSlice.config) && Objects.equals(records, hBaseSlice.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logfileTable, startOffset, ranges, config, records);
    }
}
