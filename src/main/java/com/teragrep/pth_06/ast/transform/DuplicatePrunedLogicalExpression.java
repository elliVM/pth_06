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
package com.teragrep.pth_06.ast.transform;

import com.teragrep.pth_06.ast.EmptyExpression;
import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Prunes duplicate children from a AND/OR operator
 */
public final class DuplicatePrunedLogicalExpression implements Expression, TransformedExpression {

    private final Logger LOGGER = getLogger(DuplicatePrunedLogicalExpression.class);
    private final Expression origin;

    public DuplicatePrunedLogicalExpression(final Expression origin) {
        this.origin = origin;
    }

    public Expression transformedExpression() {
        Expression optimizedExpression = origin;
        final Tag originTag = origin.tag();
        if (originTag.equals(Tag.AND) || originTag.equals(Tag.OR)) {
            final List<Expression> children = origin.children();
            final Set<Expression> seen = new HashSet<>();
            final List<Expression> uniqueExpressions = new ArrayList<>();
            // find unique expressions from children
            for (final Expression child : children) {
                if (!seen.contains(child)) {
                    seen.add(child);
                    uniqueExpressions.add(child);
                }
            }
            LOGGER.info("Removed <{}> duplicate(s) expressions", (children.size() - uniqueExpressions.size()));
            // pruned all children
            if (uniqueExpressions.isEmpty()) {
                optimizedExpression = new EmptyExpression();
            } // single child left
            else if (uniqueExpressions.size() == 1) {
                optimizedExpression = uniqueExpressions.get(0);
            } // new AND operator with pruned children
            else if (originTag.equals(Tag.AND)) {
                optimizedExpression = new AndExpression(uniqueExpressions);
            } // new OR operator with pruned children
            else {
                optimizedExpression = new OrExpression(uniqueExpressions);
            }

        }

        return optimizedExpression;
    }

    @Override
    public Tag tag() {
        return transformedExpression().tag();
    }

    @Override
    public List<Expression> children() {
        return transformedExpression().children();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        DuplicatePrunedLogicalExpression that = (DuplicatePrunedLogicalExpression) o;
        return Objects.equals(origin, that.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(origin);
    }
}
