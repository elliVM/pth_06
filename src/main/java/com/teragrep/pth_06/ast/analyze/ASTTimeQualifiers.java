package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;

import java.util.ArrayList;
import java.util.List;

public final class ASTTimeQualifiers {
    private final List<Expression> earliestList;
    private final List<Expression> latestList;
    private final Expression root;

    public ASTTimeQualifiers(final Expression root) {
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
        List<Expression> children = expression.children();
        switch (tag) {
            case OR:
                final List<Expression> traversedOrChildren = new ArrayList<>();
                for (final Expression child: children) {
                    final Expression traversedOr = findTimeQualifiers(child);
                    traversedOrChildren.add(traversedOr);
                }
                result = new OrExpression(traversedOrChildren);
                break;
            case AND:
                final List<Expression> traversedAndChildren = new ArrayList<>();
                for (final Expression child: children) {
                    final Expression traversedAnd = findTimeQualifiers(child);
                    traversedAndChildren.add(traversedAnd);
                }
                result = new AndExpression(traversedAndChildren);
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
