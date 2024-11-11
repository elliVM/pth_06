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
package com.teragrep.pth_06.planner.bloomfilter;

import org.apache.spark.util.sketch.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

/**
 * Inserts given tokens into configurable filter
 */
public final class SearchTermBloomFilter {

    private final Logger LOGGER = LoggerFactory.getLogger(SearchTermBloomFilter.class);
    private final Long expected;
    private final Double fpp;
    private final List<String> stringTokens;

    public SearchTermBloomFilter(Long expected, Double fpp, RegexExtractedValue tokenizable) {
        this(expected, fpp, tokenizable.tokens());
    }

    public SearchTermBloomFilter(Long expected, Double fpp, TokenizedValue tokenizable) {
        this(expected, fpp, new TokensAsStrings(tokenizable).tokens());
    }

    public SearchTermBloomFilter(Long expected, Double fpp, List<String> stringTokens) {
        this.expected = expected;
        this.fpp = fpp;
        this.stringTokens = stringTokens;
    }

    public byte[] bytes() {
        LOGGER.debug("Create filter from Record with values: expected <{}>, fpp <{}>", expected, fpp);
        if (stringTokens.isEmpty()) {
            throw new IllegalStateException("Tried to create a filter without any items");
        }
        final BloomFilter filter = BloomFilter.create(expected, fpp);
        for (final String token : stringTokens) {
            filter.put(token);
        }
        try (final ByteArrayOutputStream filterBAOS = new ByteArrayOutputStream()) {
            filter.writeTo(filterBAOS);
            return filterBAOS.toByteArray();
        }
        catch (final IOException e) {
            throw new UncheckedIOException("Error writing filter bytes: ", e);
        }
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null || getClass() != object.getClass())
            return false;
        final SearchTermBloomFilter cast = (SearchTermBloomFilter) object;
        return expected.equals(cast.expected) && fpp.equals(cast.fpp) && stringTokens.equals(cast.stringTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expected, fpp, stringTokens);
    }
}
