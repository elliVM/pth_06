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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** TimeQualifiers vary slightly on compute time so only tags are asserted */
public final class WithDefaultValuesTest {

    @Test
    public void testAndExpressionChildren() {
        Expression input = new AndExpression(new XMLValueExpressionImpl("host1", "equals", Expression.Tag.HOST));
        Expression transformed = new WithDefaultValues(input).transformed();
        Set<Expression.Tag> tags = transformed
                .asLogical()
                .children()
                .stream()
                .map(Expression::tag)
                .collect(Collectors.toSet());
        Set<Expression.Tag> expectedTags = new HashSet<>();
        expectedTags.add(Expression.Tag.HOST);
        expectedTags.add(Expression.Tag.INDEX);
        expectedTags.add(Expression.Tag.EARLIEST);
        expectedTags.add(Expression.Tag.LATEST);
        assertTrue(tags.containsAll(expectedTags));
    }

    @Test
    public void testOrExpressionChildren() {
        Expression value1 = new XMLValueExpressionImpl("value1", "equals", Expression.Tag.HOST);
        Expression value2 = new XMLValueExpressionImpl("value2", "equals", Expression.Tag.INDEX);
        Expression root = new OrExpression(Arrays.asList(value1, value2));
        Expression transformed = new WithDefaultValues(root).transformed();
        Assertions.assertTrue(transformed.isLogical());
        List<Expression> orChildren = transformed.asLogical().children();
        int loops = 0;
        for (Expression child : orChildren) {
            Set<Expression.Tag> tags = new HashSet<>();
            if (child.isLogical()) {
                for (Expression c : child.asLogical().children()) {
                    tags.add(c.tag());
                }
            }
            else {
                tags.add(child.tag());
            }

            // Every child of OR should have defaults: INDEX, EARLIEST, LATEST
            Assertions.assertTrue(tags.contains(Expression.Tag.INDEX), "Child missing INDEX tag");
            Assertions.assertTrue(tags.contains(Expression.Tag.EARLIEST), "Child missing EARLIEST tag");
            Assertions.assertTrue(tags.contains(Expression.Tag.LATEST), "Child missing LATEST tag");
            loops++;
        }
        Assertions.assertEquals(2, loops);
    }
}
