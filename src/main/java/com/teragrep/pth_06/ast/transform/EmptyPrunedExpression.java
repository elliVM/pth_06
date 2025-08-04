package com.teragrep.pth_06.ast.transform;

import com.teragrep.pth_06.ast.EmptyExpression;
import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Optimizes AND/OR expressions with empty values into value expressions
 */
public final class EmptyPrunedExpression implements Expression, TransformedExpression {
    private final Logger LOGGER = getLogger(EmptyPrunedExpression.class);
    private final Expression origin;

    public EmptyPrunedExpression(final Expression origin) {
        this.origin = origin;
    }

    public Expression transformedExpression() {
        Expression optimizedExpression = origin;
        final List<Expression> children = origin.children();
        final Tag originTag = origin.tag();
        final Tag emptyTag = Tag.EMPTY;
        if (originTag.equals(Tag.AND) || originTag.equals(Tag.OR)) {
            final List<Expression> childrenWithoutEmpty = new ArrayList<>();
            for (final Expression child : children) {
                if (child.tag() != emptyTag) {
                    childrenWithoutEmpty.add(child);
                }
            }
            LOGGER.info("Pruned <{}> empty expression(s)", (children.size() - childrenWithoutEmpty.size()));
            if (childrenWithoutEmpty.isEmpty()) {
                LOGGER.info("All children empty -> empty expression");
                optimizedExpression = new EmptyExpression();
            } else if (childrenWithoutEmpty.size() == 1) {
                LOGGER.info("Returning only child expression left");
                optimizedExpression = childrenWithoutEmpty.get(0);
            } else if (originTag.equals(Tag.AND)) {
                optimizedExpression = new AndExpression(childrenWithoutEmpty);
            } else {
                optimizedExpression = new OrExpression(childrenWithoutEmpty);
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
}
