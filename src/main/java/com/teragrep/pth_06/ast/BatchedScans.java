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
package com.teragrep.pth_06.ast;

import com.teragrep.pth_06.config.Config;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public final class BatchedScans {

    private final List<ScanRange> ranges;
    private final long minusHours;
    private final long quantumLength;
    private final long numPartitions;
    private final float compressionRatio;
    private final float processingSpeed;
    private final long totalObjectCountLimit;

    public BatchedScans(final Config config) {
        this(config, new ArrayList<>());
    }

    public BatchedScans(final Config config, final List<ScanRange> ranges) {
        this.ranges = ranges;
        this.minusHours = 24;
        this.quantumLength = config.batchConfig.quantumLength;
        this.numPartitions = config.batchConfig.numPartitions;
        this.compressionRatio = config.batchConfig.fileCompressionRatio;
        this.processingSpeed = config.batchConfig.processingSpeed;
        this.totalObjectCountLimit = config.batchConfig.totalObjectCountLimit;
    }

    public long getInitialOffset() {
        Long earliest = ZonedDateTime.now().minusHours(minusHours).toEpochSecond();
        for (ScanRange range : ranges) {
            if (range.earliest() < earliest) {
                earliest = range.earliest();
            }
        }
        return earliest;
    }

    public Long incrementFromOffsetAndGetLatest(Long start) {

        return null;
    }

}
