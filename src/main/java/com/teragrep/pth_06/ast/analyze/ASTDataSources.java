package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;

import java.util.ArrayList;
import java.util.List;

public final class ASTDataSources {
    private final List<Expression> dataSources;
    private final Expression root;

    public ASTDataSources(Expression root) {
        this.root = root;
        this.dataSources = new ArrayList<>();
    }

    public boolean hasDataSourceExpression() {
        return !dataSourceExpressions().isEmpty();
    }

    public List<Expression> dataSourceExpressions() {
        if (dataSources.isEmpty()) {
            findDataSources(root);
        }
        return dataSources;
    }

    // recursively apply to all expressions in AST
    private Expression findDataSources(final Expression expression) {
        final Expression.Tag tag = expression.tag();
        final Expression result;
        final List<Expression> children = expression.children();
        switch (tag) {
            case OR:
                final List<Expression> traversedOrChildren = new ArrayList<>();
                for (final Expression child: children) {
                    final Expression traversedOr = findDataSources(child);
                    traversedOrChildren.add(traversedOr);
                }
                result = new OrExpression(traversedOrChildren);
                break;
            case AND:
                final List<Expression> traversedAndChildren = new ArrayList<>();
                for (final Expression child: children) {
                    final Expression traversedAnd = findDataSources(child);
                    traversedAndChildren.add(traversedAnd);
                }
                result = new AndExpression(traversedAndChildren);
                break;
            case INDEX:
            case HOST:
            case SOURCETYPE:
                dataSources.add(expression);
                result = expression;
                break;
            case EARLIEST:
            case LATEST:
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
