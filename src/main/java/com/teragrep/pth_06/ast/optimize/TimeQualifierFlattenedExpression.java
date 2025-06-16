package com.teragrep.pth_06.ast.optimize;

import com.teragrep.pth_06.ast.BinaryExpression;
import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.ValueExpression;
import com.teragrep.pth_06.ast.xml.ValueExpressionImpl;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public final  class TimeQualifierFlattenedExpression implements OptimizedExpression {
    private final Logger LOGGER = getLogger(TimeQualifierFlattenedExpression.class);
    private final Expression origin;

    public TimeQualifierFlattenedExpression(Expression origin) {
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
            if (leftTag.equals(Expression.Tag.EARLIEST) && rightTag.equals(Expression.Tag.EARLIEST)) {
                ValueExpression leftValueExpression = (ValueExpression) left;
                ValueExpression rightValueExpression = (ValueExpression) right;
                long leftLongValue = Long.parseLong(leftValueExpression.value());
                long rightLongValue = Long.parseLong(rightValueExpression.value());
                String maxLongString = String.valueOf(Math.max(leftLongValue, rightLongValue));
                optimizedExpression = new ValueExpressionImpl(maxLongString, "EQUALS", Expression.Tag.EARLIEST);
            } else if (leftTag.equals(Expression.Tag.LATEST) && rightTag.equals(Expression.Tag.LATEST)) {
                ValueExpression leftValueExpression = (ValueExpression) left;
                ValueExpression rightValueExpression = (ValueExpression) right;
                long leftLongValue = Long.parseLong(leftValueExpression.value());
                long rightLongValue = Long.parseLong(rightValueExpression.value());
                String maxLongString = String.valueOf(Math.min(leftLongValue, rightLongValue));
                optimizedExpression = new ValueExpressionImpl(maxLongString, "EQUALS", Expression.Tag.LATEST);
            }
        }
        return optimizedExpression;
    }

}
