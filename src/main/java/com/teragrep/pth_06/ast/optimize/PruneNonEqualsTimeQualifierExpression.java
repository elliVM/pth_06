package com.teragrep.pth_06.ast.optimize;

import com.teragrep.pth_06.ast.BinaryExpression;
import com.teragrep.pth_06.ast.EmptyExpression;
import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.ValueExpression;

public final class PruneNonEqualsTimeQualifierExpression implements OptimizedExpression {
    private final Expression origin;

    public PruneNonEqualsTimeQualifierExpression(Expression origin) {
        this.origin = origin;
    }

    public Expression optimizedExpression() {
        Expression optimizedExpression = origin;
        if (origin.tag().equals(Expression.Tag.AND) || origin.tag().equals(Expression.Tag.AND)) {
            BinaryExpression binaryExpression = (BinaryExpression) origin;
            Expression left = binaryExpression.left();
            Expression right = binaryExpression.right();
            Expression.Tag leftTag = left.tag();
            Expression.Tag rightTag = right.tag();
            if (leftTag.equals(Expression.Tag.EARLIEST) || leftTag.equals(Expression.Tag.LATEST)) {
                ValueExpression leftValueExpression = (ValueExpression) left;
                String leftOperation = leftValueExpression.operation();
                if (!"EQUALS".equalsIgnoreCase(leftOperation)) {
                    optimizedExpression = new EmptyExpression();
                }
            } else if (rightTag.equals(Expression.Tag.EARLIEST) || rightTag.equals(Expression.Tag.LATEST)) {
                ValueExpression rightValueExpression = (ValueExpression) left;
                String rightOperation = rightValueExpression.operation();
                if (!"EQUALS".equalsIgnoreCase(rightOperation)) {
                    optimizedExpression = new EmptyExpression();
                }
            }
        }
        return optimizedExpression;
    }
}
