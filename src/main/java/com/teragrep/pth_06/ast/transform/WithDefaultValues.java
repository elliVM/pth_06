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

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;
import com.teragrep.pth_06.ast.xml.XMLValueExpressionImpl;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class WithDefaultValues implements ExpressionTransformation<Expression> {

    private final Expression root;

    public WithDefaultValues(final Expression root) {
        this.root = root;
    }

    @Override
    public Expression transformed() {
        return withDefaults(root);
    }

    private Expression withDefaults(final Expression expression) {
        if (!expression.isLogical()) {
            return expression;
        }
        final Expression withDefaults;
        final Expression.Tag tag = expression.tag();
        if (tag.equals(Expression.Tag.AND)) {
            withDefaults = andWithDefaults(expression);
        }
        else {
            withDefaults = orWithDefaults(expression);
        }
        return withDefaults;
    }

    private Expression andWithDefaults(Expression expression) {
        final List<Expression> andChildren = new ArrayList<>();
        for (final Expression child : expression.asLogical().children()) {
            andChildren.add(withDefaults(child));
        }
        final Set<Expression.Tag> tags = expressionTags(new AndExpression(andChildren));
        if (!tags.contains(Expression.Tag.INDEX)) {
            andChildren.add(new XMLValueExpressionImpl("*", "equals", Expression.Tag.INDEX));
        }
        if (!tags.contains(Expression.Tag.EARLIEST)) {
            andChildren
                    .add(new XMLValueExpressionImpl(ZonedDateTime.now().minusDays(1).toString(), "equals", Expression.Tag.EARLIEST));
        }
        if (!tags.contains(Expression.Tag.LATEST)) {
            andChildren
                    .add(new XMLValueExpressionImpl(ZonedDateTime.now().toString(), "equals", Expression.Tag.LATEST));
        }
        return new AndExpression(andChildren);
    }

    private Expression orWithDefaults(Expression expression) {
        List<Expression> orChildrenWithDefaults = new ArrayList<>();
        for (Expression child : expression.asLogical().children()) {
            Expression childWithDefaults = withDefaults(child);

            // Add defaults to the child if missing
            Set<Expression.Tag> tags = expressionTags(childWithDefaults); // your helper or similar

            List<Expression> childExpressions = new ArrayList<>();
            if (childWithDefaults.isLogical() && childWithDefaults.tag().equals(Expression.Tag.AND)) {
                childExpressions.addAll(childWithDefaults.asLogical().children());
            }
            else {
                childExpressions.add(childWithDefaults);
            }

            if (!tags.contains(Expression.Tag.INDEX)) {
                childExpressions.add(new XMLValueExpressionImpl("*", "equals", Expression.Tag.INDEX));
            }
            if (!tags.contains(Expression.Tag.EARLIEST)) {
                childExpressions
                        .add(new XMLValueExpressionImpl(ZonedDateTime.now().minusDays(1).toString(), "equals", Expression.Tag.EARLIEST));
            }
            if (!tags.contains(Expression.Tag.LATEST)) {
                childExpressions
                        .add(new XMLValueExpressionImpl(ZonedDateTime.now().toString(), "equals", Expression.Tag.LATEST));
            }

            // Wrap child expressions into an AND (or single expression if only one)
            Expression newChild = (childExpressions.size() == 1) ? childExpressions
                    .get(0) : new AndExpression(childExpressions);
            orChildrenWithDefaults.add(newChild);
        }
        return new OrExpression(orChildrenWithDefaults);
    }

    private Set<Expression.Tag> expressionTags(final Expression expr) {
        Set<Expression.Tag> tags = new HashSet<>();
        if (expr.isLogical()) {
            for (Expression child : expr.asLogical().children()) {
                tags.addAll(expressionTags(child));
            }
        }
        else {
            tags.add(expr.tag());
        }
        return tags;
    }
}
