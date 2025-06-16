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
package com.teragrep.pth_06.ast.optimize;

import com.teragrep.pth_06.ast.EmptyExpression;
import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;
import com.teragrep.pth_06.ast.xml.ValueExpressionImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class CombinedOptimizedXMLExpressionTest {

    @Test
    public void testASTOptimization() {
        ValueExpressionImpl indexExp = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        ValueExpressionImpl earliestExp = new ValueExpressionImpl("test_2", "equals", Expression.Tag.EARLIEST);
        Expression ast = new AndExpression(
                new OrExpression(indexExp, indexExp),
                new AndExpression(earliestExp, earliestExp)
        );
        Expression optimizedAST = new CombinedOptimizedXMLExpression(ast).optimizedExpression();
        Expression expectedAST = new AndExpression(indexExp, earliestExp);
        Assertions.assertEquals(expectedAST, optimizedAST);
    }

    @Test
    public void testEqualBinaryMembersToValue() {
        ValueExpressionImpl indexExp = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        Expression ast = new AndExpression(
                indexExp,
                indexExp
        );
        Expression optimizedAST = new CombinedOptimizedXMLExpression(ast).optimizedExpression();
        Assertions.assertEquals(indexExp, optimizedAST);
    }

    @Test
    public void testSingleDepthNestedBinary() {
        ValueExpressionImpl indexExp = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        Expression ast = new AndExpression(
                new OrExpression(indexExp, indexExp),
                indexExp
        );
        Expression optimizedAST = new CombinedOptimizedXMLExpression(ast).optimizedExpression();
        Assertions.assertEquals(indexExp, optimizedAST);
    }

    @Test
    public void testMultipleDepthNestedBinaryExpressionsToValue() {
        ValueExpressionImpl indexExp = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        Expression ast = new AndExpression(
                new AndExpression(
                        new AndExpression(indexExp, indexExp),
                        new AndExpression(indexExp, indexExp)
                ), new AndExpression(indexExp, indexExp)
        );
        Expression optimizedAST = new CombinedOptimizedXMLExpression(ast).optimizedExpression();
        Assertions.assertEquals(indexExp, optimizedAST);
    }

    @Test
    public void testUnbalancedNestedBinaryExpressionsToValue() {
        ValueExpressionImpl indexExp = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        Expression ast = new AndExpression(
                new AndExpression(
                        new AndExpression(indexExp, indexExp),
                        indexExp
                ), indexExp
        );
        Expression optimizedAST = new CombinedOptimizedXMLExpression(ast).optimizedExpression();
        Assertions.assertEquals(indexExp, optimizedAST);
    }

    @Test
    public void testUnbalancedNestedEqualBinaryWithEmpty() {
        ValueExpressionImpl indexExp = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        Expression ast = new AndExpression(
                new AndExpression(
                        new AndExpression(indexExp, indexExp),
                        new EmptyExpression()
                ), indexExp
        );
        Expression optimizedAST = new CombinedOptimizedXMLExpression(ast).optimizedExpression();
        Assertions.assertEquals(indexExp, optimizedAST);
    }
}
