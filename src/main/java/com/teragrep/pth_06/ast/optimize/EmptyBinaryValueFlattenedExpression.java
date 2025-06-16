package com.teragrep.pth_06.ast.optimize;

import com.teragrep.pth_06.ast.BinaryExpression;
import com.teragrep.pth_06.ast.EmptyExpression;
import com.teragrep.pth_06.ast.Expression;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/** Optimizes AND/OR expressions with empty values into value expressions */
public final class EmptyBinaryValueFlattenedExpression implements Expression, OptimizedExpression {
    private final Logger LOGGER = getLogger(EmptyBinaryValueFlattenedExpression.class);
    private final Expression origin;

    public EmptyBinaryValueFlattenedExpression(final Expression origin) {
        this.origin = origin;
    }

    public Expression optimizedExpression() {
        Expression optimizedExpression = origin;
        Tag originTag = origin.tag();
        if (originTag.equals(Tag.AND) || originTag.equals(Tag.OR)) {
            BinaryExpression binaryExpression = (BinaryExpression) origin;
            Expression left = binaryExpression.left();
            Expression right = binaryExpression.right();
            Expression.Tag leftTag = left.tag();
            Expression.Tag rightTag = right.tag();
            Expression.Tag empty = Expression.Tag.EMPTY;
            if (leftTag == empty && rightTag == empty) {
                LOGGER.info("AND/OR(EMPTY, EMPTY) -> EMPTY");
                optimizedExpression = new EmptyExpression();
            }
            else if (leftTag == empty) {
                LOGGER.info("AND/OR(EMPTY, X) -> X");
                optimizedExpression = right;
            }
            else if (rightTag == empty) {
                LOGGER.info("AND/OR(X, EMPTY) -> X");
                optimizedExpression = left;
            }
        }
        return optimizedExpression;
    }

    @Override
    public Tag tag() {
        return optimizedExpression().tag();
    }

    @Override
    public List<Expression> children() {
        return optimizedExpression().children();
    }
}
