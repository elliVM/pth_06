package com.teragrep.pth_06.ast.transform;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.ValueExpression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;

import java.util.List;
import java.util.stream.Collectors;

public final class PruneNonEqualsTimeQualifierExpression implements TransformedExpression {
    private final Expression origin;

    public PruneNonEqualsTimeQualifierExpression(Expression origin) {
        this.origin = origin;
    }

    public Expression transformedExpression() {
        Expression optimizedExpression = origin;
        final List<Expression> children = origin.children();
        final Expression.Tag originTag = origin.tag();
        if (originTag.equals(Expression.Tag.AND) || originTag.equals(Expression.Tag.OR)) {
            final List<Expression> prunedChildren = children.stream().filter(expression -> {
                        Expression.Tag tag = expression.tag();
                        if (tag.equals(Expression.Tag.EARLIEST) || tag.equals(Expression.Tag.LATEST)) {
                            ValueExpression valueExpression = (ValueExpression) expression;
                            String operation = valueExpression.operation();
                            return "EQUALS".equalsIgnoreCase(operation);
                        }
                        return true;
                    })
                    .collect(Collectors.toList());
            if (originTag.equals(Expression.Tag.AND)) {
                optimizedExpression = new AndExpression(prunedChildren);
            } else {
                optimizedExpression = new OrExpression(prunedChildren);
            }
        }
        return optimizedExpression;
    }
}
