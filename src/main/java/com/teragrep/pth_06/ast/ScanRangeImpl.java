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

import com.teragrep.pth_06.Stubbable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Objects;

public final class ScanRangeImpl implements ScanRange, Stubbable {

    private final Long streamId;
    private final Long earliest;
    private final Long latest;
    private final FilterList filterList;

    public ScanRangeImpl(final Long streamId, final Long earliest, final Long latest, final FilterList filterList) {
        this.streamId = streamId;
        this.earliest = earliest;
        this.latest = latest;
        this.filterList = filterList;
    }

    @Override
    public Scan toScan() {
        byte[] startRow = Bytes.add(Bytes.toBytes(streamId), Bytes.toBytes(earliest));
        byte[] stopRow = Bytes.add(Bytes.toBytes(streamId), Bytes.toBytes(latest + 1)); // inclusive
        Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);
        scan.setFilter(filterList);
        return scan;
    }

    @Override
    public ScanRange rangeFromEarliest(Long earliestLimit) {
        if (this.earliest < earliestLimit && earliestLimit < latest) {
            return new ScanRangeImpl(streamId, earliestLimit, latest, filterList);
        }
        else {
            return this;
        }
    }

    @Override
    public ScanRange rangeUntilLatest(Long latestLimit) {
        if (earliest < latestLimit && latestLimit < latest) {
            return new ScanRangeImpl(streamId, latestLimit, latest, filterList);
        }
        else {
            return this;
        }
    }

    @Override
    public ScanRange toRangeBetween(Long earliestLimit, Long latestLimit) {
        boolean rangeIntersects = new ScanRangeImpl(streamId, earliestLimit, latestLimit, filterList).intersects(this);
        if (rangeIntersects) {
            Long updatedEarliest = earliest;
            Long updatedLatest = earliest;
            if (earliestLimit > earliest) {
                updatedEarliest = earliestLimit;
            }
            if (latestLimit < latest) {
                updatedLatest = earliestLimit;
            }
            return rangeFromEarliest(updatedEarliest).rangeUntilLatest(updatedLatest);
        }
        else {
            return new StubScanRange();
        }
    }

    public boolean intersects(final ScanRange other) {
        if (!Objects.equals(this.streamId, other.streamId()) || !filterList.equals(other.filterList())) {
            return false;
        }
        return this.earliest <= other.latest() && other.earliest() <= this.latest;
    }

    public ScanRangeImpl merge(final ScanRange other) {
        if (intersects(other)) {
            final Long minEarliest = Math.min(earliest, other.earliest());
            final Long maxLatest = Math.max(latest, other.latest());
            return new ScanRangeImpl(streamId, minEarliest, maxLatest, filterList);
        }
        else {
            throw new IllegalArgumentException("Unable to merge ranges did not intersect");
        }
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final ScanRangeImpl scanRangeImpl = (ScanRangeImpl) object;
        return Objects.equals(streamId, scanRangeImpl.streamId) && Objects.equals(earliest, scanRangeImpl.earliest)
                && Objects.equals(latest, scanRangeImpl.latest) && Objects.equals(filterList, scanRangeImpl.filterList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, earliest, latest, filterList);
    }

    @Override
    public String toString() {
        return String.format("ScanRange id: <%s> between <%s> - <%s>", streamId, earliest, latest);
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public Long streamId() {
        return streamId;
    }

    @Override
    public Long earliest() {
        return earliest;
    }

    @Override
    public Long latest() {
        return latest;
    }

    @Override
    public FilterList filterList() {
        return filterList;
    }
}
