package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.OrExpression;

import java.util.ArrayList;
import java.util.List;

public final class DataSourceExpressions {
    private final List<Expression> dataSources;
    private final Expression root;

    public DataSourceExpressions(Expression root) {
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
        switch (tag) {
            case OR:
                final OrExpression or = (OrExpression) expression;
                final Expression orLeft = findDataSources(or.left());
                final Expression orRight = findDataSources(or.right());
                result = new OrExpression(orLeft, orRight);
                break;
            case AND:
                final AndExpression and = (AndExpression) expression;
                final Expression andLeft = findDataSources(and.left());
                final Expression andRight = findDataSources(and.right());
                result = new AndExpression(andLeft, andRight);
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
