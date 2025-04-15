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
package com.teragrep.pth_06.planner.walker.conditions;

import com.teragrep.pth_06.config.ConditionConfig;
import org.jooq.Condition;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.Objects;
import java.util.Set;

/**
 * Creates a query condition from provided dom element
 */
public final class ElementCondition implements QueryCondition, BloomQueryCondition {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElementCondition.class);

    private final ValidElement element;
    private final ConditionConfig config;

    public ElementCondition(final Element element, final ConditionConfig config) {
        this(new ValidElement(element), config);
    }

    public ElementCondition(final ValidElement element, final ConditionConfig config) {
        this.element = element;
        this.config = config;
    }

    public Condition condition() {
        final String tag = element.tag();
        final String value = element.value();
        final String operation = element.operation();
        final boolean isStreamQuery = config.streamQuery();
        final Condition condition;
        if (isStreamQuery) {
            switch (tag.toLowerCase()) {
                case "index":
                    final QueryCondition index = new IndexCondition(value, operation, true);
                    condition = index.condition();
                    break;
                case "sourcetype":
                    final QueryCondition sourceType = new SourceTypeCondition(value, operation, true);
                    condition = sourceType.condition();
                    break;
                case "host":
                    final QueryCondition host = new HostCondition(value, operation, true);
                    condition = host.condition();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported element tag for stream query <" + tag + ">");
            }
        }
        else {
            switch (tag.toLowerCase()) {
                case "index":
                    final QueryCondition index = new IndexCondition(value, operation, false);
                    condition = index.condition();
                    break;
                case "sourcetype":
                    final QueryCondition sourceType = new SourceTypeCondition(value, operation, false);
                    condition = sourceType.condition();
                    break;
                case "host":
                    final QueryCondition host = new HostCondition(value, operation, false);
                    condition = host.condition();
                    break;
                case "earliest":
                case "index_earliest":
                    final QueryCondition earliest = new EarliestCondition(value);
                    condition = earliest.condition();
                    break;
                case "latest":
                case "index_latest":
                    final QueryCondition latest = new LatestCondition(value);
                    condition = latest.condition();
                    break;
                case "indexstatement":
                    final QueryCondition indexStatement = new IndexStatementCondition(value, operation, config);
                    condition = indexStatement.condition();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported element tag <" + tag + ">");
            }
        }
        LOGGER.debug("Query condition from element <{}> = <{}>", element, condition);
        return condition;
    }

    public boolean isBloomSearchCondition() {
        final String tag = element.tag();
        final String operation = element.operation();
        return "indexstatement".equalsIgnoreCase(tag) && "EQUALS".equals(operation) && !config.streamQuery()
                && config.bloomEnabled();
    }

    /**
     * A set of tables needed to be joined to the query to use this condition
     */
    public Set<Table<?>> requiredTables() {
        final String value = element.value();
        return new IndexStatementCondition(value, config).requiredTables();
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (object.getClass() != this.getClass()) {
            return false;
        }
        final ElementCondition cast = (ElementCondition) object;
        return this.element.equals(cast.element) && this.config.equals(cast.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(element, config);
    }
}
