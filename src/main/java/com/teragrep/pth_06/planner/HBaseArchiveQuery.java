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
import com.teragrep.pth_06.ast.analyze.ScanRanges;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.walker.EarliestWalker;
import org.apache.hadoop.hbase.client.Table;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.sql.Date;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

public final class HBaseArchiveQuery implements ArchiveQuery {

    private final Logger LOGGER = LoggerFactory.getLogger(HBaseArchiveQuery.class);

    private final Config config;
    private final List<ScanRange> ranges;
    private final LinkedList<Slice> sliceBuffer = new LinkedList<>();
    private final TreeMap<Long, Slice> hourlyResults;
    private final Table logfileTable;
    private final long queryStartTimeEpoch;
    private long latest = -1;

    public HBaseArchiveQuery(final Config config) {
        this(
                config,
                new ScanRanges(config),
                new TreeMap<>(),
                new LogfileTable(config),
                ZonedDateTime.now().toEpochSecond()
        );
    }

    public HBaseArchiveQuery(final Config config, long queryStartTimeEpoch) {
        this(config, new ScanRanges(config), new TreeMap<>(), new LogfileTable(config), queryStartTimeEpoch);
    }

    public HBaseArchiveQuery(
            final Config config,
            final ScanRanges scanRanges,
            final TreeMap<Long, Slice> hourlyResults,
            final LogfileTable logfileTable,
            final long queryStartTimeEpoch
    ) {
        this(config, scanRanges.rangeList(), hourlyResults, logfileTable.table(), queryStartTimeEpoch);
    }

    public HBaseArchiveQuery(
            final Config config,
            final List<ScanRange> ranges,
            final TreeMap<Long, Slice> hourlyResults,
            final Table logfileTable,
            final long queryStartTimeEpoch
    ) {
        this.config = config;
        this.ranges = ranges;
        this.hourlyResults = hourlyResults;
        this.logfileTable = logfileTable;
        this.queryStartTimeEpoch = queryStartTimeEpoch;
    }

    /**
     * Fetches the logfiles for the given offset range.
     *
     * @param startHour
     * @param endHour
     * @return
     */
    @Override
    public Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> processBetweenUnixEpochHours(
            long startHour,
            long endHour
    ) {
        final Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> result;
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
        result = using
                .newResult(
                        idField, directoryField, streamField, hostField, logtagField, logdateField, bucketField,
                        pathField, logtimeField, filesizeField, uncompressedFilesizeField
                );

        long slices = 0;
        for (final Slice slice: sliceBuffer) {
            if (slice.start() >= startHour && slice.start() < endHour) {
                if (slice.stop() > endHour) {
                    LOGGER.warn("Slice end was outside of end hour");
                }
                slices++;
                result.addAll(slice.asResult());
            }
        }
        LOGGER.info("Processing between <{}>-<{}>, iterated <{}> slices from buffer, added result with <{}> size", startHour, endHour, slices, result.size());
        return result;
    }

    /**
     * Informs the source that Spark has completed processing all data for offsets less than or equal to `end` and will
     * only request offsets greater than `end` in the future.
     *
     * @param offset
     */
    @Override
    public void commit(long offset) {
        LOGGER.debug("Processed until offset <{}>", offset);
        sliceBuffer.removeIf(slice -> slice.stop() <= offset);
        hourlyResults.headMap(offset, true).clear();
    }

    /**
     * Used when Spark requests the initial offset when starting a new query.
     *
     * @return
     */
    @Override
    public Long getInitialOffset() {
        Long earliest;
        if (!ranges.isEmpty()) {
            earliest = Long.MAX_VALUE;
            for (final ScanRange range : ranges) {
                if (range.earliest() < earliest) {
                    earliest = range.earliest();
                }
            }
        }
        else {
            try { // use walker if no scan ranges provided
                earliest = new EarliestWalker().fromString(config.query);
            }
            catch (ParserConfigurationException | IOException | SAXException e) {
                earliest = 0L;
            }
        }
        LOGGER.info("initial offset <{}>", earliest);
        return earliest;
    }

    /**
     * Used when Spark progresses the query further to fetch more data.
     *
     * @return offset for the latest logfile to read
     */
    @Override
    public Long incrementAndGetLatestOffset() {
        if (latest < 0 ){
            latest = getInitialOffset();
        }
        final long maxWeight = (long) config.batchConfig.quantumLength * config.batchConfig.numPartitions;
        final BatchSizeLimit batchSizeLimit = new BatchSizeLimit(maxWeight, config.batchConfig.totalObjectCountLimit);
        final long stopOffset = stopOffset();

        while(!batchSizeLimit.isOverLimit() && latest < stopOffset) {
            long sliceEnd = Math.min(latest + 3600, stopOffset);
            Slice slice = new HBaseSlice(logfileTable, latest, sliceEnd, ranges, config);
            WeightedOffset weightedOffset = slice.weightedOffset();

            if (!weightedOffset.isStub) {
                sliceBuffer.add(slice);
                batchSizeLimit.add(weightedOffset.estimateWeight(config.batchConfig.fileCompressionRatio, config.batchConfig.processingSpeed));
            }
            latest = sliceEnd;
        }
        return latest;
    }

    public Long incrementAndGetLatestOffsetOld() {
        final long stopOffset = stopOffset();
        final int quantumLength = config.batchConfig.quantumLength;
        final long maxWeight = (long) quantumLength * config.batchConfig.numPartitions;

        Long latest;
        if (hourlyResults.isEmpty()) {
            latest = getInitialOffset();
        }
        else {
            latest = hourlyResults.lastEntry().getKey();
            LOGGER.info("Got latest processed epoch as <{}>", latest);
        }

        // Initialize the batchSizeLimit object to split the data into appropriate sized batches
        final BatchSizeLimit batchSizeLimit = new BatchSizeLimit(maxWeight, config.batchConfig.totalObjectCountLimit);

        while (!batchSizeLimit.isOverLimit()) {

            if (latest >= stopOffset) {
                LOGGER.info("Reached stop offset <{}>", stopOffset);
                break;
            }

            long start = latest;
            latest += 3600L;

            if (latest > stopOffset) {
                latest = stopOffset;
            }

            final Slice slice = new HBaseSlice(logfileTable, start, latest, ranges, config);
            hourlyResults.put(latest, slice);

            final WeightedOffset weightedOffset = hourlyResults.get(latest).weightedOffset();

            if (!weightedOffset.isStub) {
                LOGGER.info("Adding file size for latest offset <{}>", latest);
                batchSizeLimit
                        .add(
                                weightedOffset
                                        .estimateWeight(
                                                config.batchConfig.fileCompressionRatio,
                                                config.batchConfig.processingSpeed
                                        )
                        );
            }
        }

        return latest;
    }

    private long stopOffset() {
        Long stopOffset;
        if (!ranges.isEmpty()) {
            stopOffset = Long.MIN_VALUE;
            for (final ScanRange range : ranges) {
                final Long latest = range.latest();
                if (latest > stopOffset) {
                    stopOffset = latest;
                }
            }
        }
        else {
            stopOffset = config.archiveConfig.archiveIncludeBeforeEpoch;
            LOGGER.info("no latest offset found in query using archive include before epoch <{}>", stopOffset);
        }
        // do not query pass the query start times
        if (stopOffset > queryStartTimeEpoch) {
            LOGGER
                    .info(
                            "stop offset <{}>, passed the query start time, limiting to <{}>", stopOffset,
                            queryStartTimeEpoch
                    );
            stopOffset = queryStartTimeEpoch;
        }
        return stopOffset;
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
