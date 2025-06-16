package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;

import java.util.ArrayList;
import java.util.List;

public final class TimeQualifierExpressions {
    private final List<Expression> earliestList;
    private final List<Expression> latestList;
    private final Expression root;

    public TimeQualifierExpressions(final Expression root) {
        this.root = root;
        this.earliestList = new ArrayList<>();
        this.latestList = new ArrayList<>();
    }

    public boolean hasEarliestExpressions() {
        return !earliestList.isEmpty();
    }

    public boolean hasLatestExpressions() {
        return !latestList.isEmpty();
    }

    public List<Expression> earliestExpressions() {
        if (earliestList.isEmpty()) {
            findTimeQualifiers(root);
        }
        return earliestList;
    }

    public List<Expression> latestExpressions() {
        if (latestList.isEmpty()) {
            findTimeQualifiers(root);
        }
        return latestList;
    }

    // recursively traverse AST
    private Expression findTimeQualifiers(final Expression expression) {
        final Expression.Tag tag = expression.tag();
        final Expression result;
        switch (tag) {
            case OR:
                final OrExpression or = (OrExpression) expression;
                final Expression orLeft = findTimeQualifiers(or.left());
                final Expression orRight = findTimeQualifiers(or.right());
                result = new OrExpression(orLeft, orRight);
                break;
            case AND:
                final AndExpression and = (AndExpression) expression;
                final Expression andLeft = findTimeQualifiers(and.left());
                final Expression andRight = findTimeQualifiers(and.right());
                result = new AndExpression(andLeft, andRight);
                break;
            case EARLIEST:
                earliestList.add(expression);
                result = expression;
                break;
            case LATEST:
                latestList.add(expression);
                result = expression;
                break;
            case INDEX:
            case HOST:
            case SOURCETYPE:
            case INDEXSTATEMENT:
            case EMPTY:
                result = expression;
                break;
            default:
                throw new IllegalArgumentException("Unsupported tag <" + tag + ">");
        }
        return result;
    }
}
