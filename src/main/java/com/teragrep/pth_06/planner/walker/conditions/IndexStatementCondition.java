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
import com.teragrep.pth_06.planner.bloomfilter.*;
import org.jooq.Condition;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb.BLOOMDB;

public final class IndexStatementCondition implements QueryCondition, BloomQueryCondition {

    private final Logger LOGGER = LoggerFactory.getLogger(IndexStatementCondition.class);

    private final String value;
    private final String operation;
    private final ConditionConfig config;
    private final Set<Table<?>> requiredTables;

    public IndexStatementCondition(final String value, final ConditionConfig config) {
        this(value, "EQUALS", config);
    }

    public IndexStatementCondition(final String value, final String operation, final ConditionConfig config) {
        this(value, operation, config, new HashSet<>());
    }

    public IndexStatementCondition(
            final String value,
            final String operation,
            final ConditionConfig config,
            final Set<Table<?>> requiredTables
    ) {
        this.value = value;
        this.operation = operation;
        this.config = config;
        this.requiredTables = requiredTables;
    }

    public Condition condition() {
        Condition newCondition = DSL.noCondition();

        if (!"EQUALS".equals(operation) || !config.bloomEnabled()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER
                        .debug(
                                "Indexstatement reached without equals operation or bloom disabled. (operation=<{}>, bloom.enabled=<{}>)",
                                operation, config.bloomEnabled()
                        );
            }
            return newCondition;
        }

        if (requiredTables.isEmpty()) {
            // get all tables that pattern match with search value
            final QueryCondition regexLikeCondition = new RegexLikeCondition(value, BLOOMDB.FILTERTYPE.PATTERN);
            final DatabaseTables patternMatchTables = new ConditionMatchBloomDBTables(
                    config.context(),
                    regexLikeCondition
            );
            requiredTables.addAll(patternMatchTables.tables());
        }

        if (!requiredTables.isEmpty()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Found pattern match on <{}> table(s)", requiredTables.size());
            }
            Condition combinedTableCondition = DSL.noCondition();
            Condition combinedNullFilterCondition = DSL.noCondition();

            for (final Table<?> table : requiredTables) {
                // create a category temp table with filters
                final CategoryTable categoryTable = new CategoryTableWithFilters(
                        config.context(),
                        table,
                        config.bloomTermId(),
                        value
                );
                categoryTable.create();
                // create table condition for table
                final Condition nullFilterCondition = table.field("filter").isNull();
                final QueryCondition tableCondition = new CategoryTableCondition(table, config.bloomTermId());
                combinedTableCondition = combinedTableCondition.or(tableCondition.condition());
                combinedNullFilterCondition = combinedNullFilterCondition.and(nullFilterCondition);
            }
            if (config.withoutFilters()) {
                newCondition = combinedNullFilterCondition;
            }
            else {
                newCondition = combinedTableCondition.or(combinedNullFilterCondition);
            }
        }
        return newCondition;
    }

    @Override
    public boolean isBloomSearchCondition() {
        return config.bloomEnabled() && !config.streamQuery();
    }

    @Override
    public Set<Table<?>> requiredTables() {
        if (requiredTables.isEmpty()) {
            condition();
        }
        return requiredTables;
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
        final IndexStatementCondition cast = (IndexStatementCondition) object;
        return this.value.equals(cast.value) && this.config.equals(cast.config) && this.operation.equals(cast.operation)
                && this.requiredTables.equals(cast.requiredTables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, operation, config, requiredTables);
    }
}
