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
import com.teragrep.pth_06.ast.transform.OptimizedAST;
import com.teragrep.pth_06.ast.transform.TransformToScanGroups;
import com.teragrep.pth_06.ast.xml.XMLQuery;
import com.teragrep.pth_06.config.Config;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class HBaseArchiveQuery implements ArchiveQuery {

    private final Logger LOGGER = LoggerFactory.getLogger(HBaseArchiveQuery.class);
    private final Config config;
    private final List<ScanRange> ranges;
    private final Map<Long, EpochHourResult> hourlyResults;


    public HBaseArchiveQuery(final Config config) {
        this(config, new ScanRanges(new TransformToScanGroups(new OptimizedAST(new XMLQuery(config.query)))).rangeList(), new TreeMap<>());
    }

    public HBaseArchiveQuery(final Config config, final List<ScanRange> ranges, Map<Long, EpochHourResult> hourlyResults) {
        this.config = config;
        this.ranges = ranges;
        this.hourlyResults =  hourlyResults;
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
        return hourlyResults.get(startHour).toJooqResults();
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
    }

    /**
     * Used when Spark requests the initial offset when starting a new query.
     *
     * @return
     */
    @Override
    public Long getInitialOffset() {
        return 0L;
    }

    /**
     * Used when Spark progresses the query further to fetch more data.
     *
     * @return offset for the latest logfile to read
     */
    @Override
    public Long incrementAndGetLatestOffset() {
        long accumulatedSize = 0;
        long currentHour;
        return 0L;
    }
}
