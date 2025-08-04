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
import com.teragrep.pth_06.ast.PrintAST;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class CombinedTransformedXMLExpression implements TransformedExpression {
    private final Logger LOGGER = LoggerFactory.getLogger(CombinedTransformedXMLExpression.class);
    private final Expression root;

    public CombinedTransformedXMLExpression(Expression root) {
        this.root = root;
    }

    public Expression transformedExpression() {
        Expression current = root;
        Expression last;
        LOGGER.info("Start AST:\n {}",new PrintAST(root).format());
        do { // apply until no optimization changes occur
            last = current;
            current = applyOptimizations(current);
        } while(!current.equals(last));
        LOGGER.info("Final AST:\n {}",new PrintAST(current).format());
        return current;
    }

    // recursively apply to all expressions in AST
    private Expression applyOptimizations(Expression expression) {
        Expression.Tag tag = expression.tag();
        Expression result;
        switch (tag) {
            case OR:
                final List<Expression> orChildren = expression.children();
                final List<Expression> optimizedOrChildren = new ArrayList<>();
                for(final Expression child: orChildren) {
                    final Expression optimized = applyOptimizations(child);
                    optimizedOrChildren.add(optimized);
                }
                result = new OrExpression(optimizedOrChildren);
                break;
            case AND:
                final List<Expression> andChildren = expression.children();
                final List<Expression> optimizedAndChildren = new ArrayList<>();
                for(final Expression child: andChildren) {
                    final Expression optimized = applyOptimizations(child);
                    optimizedAndChildren.add(optimized);
                }
                result = new AndExpression(optimizedAndChildren);
                break;
            case EARLIEST:
            case LATEST:
            case INDEX:
            case INDEXSTATEMENT:
            case HOST:
            case SOURCETYPE:
            case EMPTY:
                result = expression;
                break;
            default:
                throw new IllegalArgumentException("Unsupported tag <" + tag + ">");
        }
        return onceTransformedExpression(result);
    }

    // apply optimizations once for a single expression
    private Expression onceTransformedExpression(Expression expression) {
        Expression result = new DuplicatePrunedLogicalExpression(expression).transformedExpression();
        result = new PruneNonEqualsTimeQualifierExpression(result).transformedExpression();
        result = new EmptyPrunedExpression(result).transformedExpression();
        return result;
    }
}
