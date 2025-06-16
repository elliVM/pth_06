package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.ValueExpressionImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public final class DataSourceExpressionsTest {

    @Test
    public void testNoDataSourceValues() {
        AndExpression ast = new AndExpression(
                new ValueExpressionImpl("100", "equals", Expression.Tag.EARLIEST),
                new ValueExpressionImpl("10000", "equals", Expression.Tag.LATEST)
        );
        DataSourceExpressions dataSourceExpressions = new DataSourceExpressions(ast);
        List<Expression> datasSourceExpressions = dataSourceExpressions.dataSourceExpressions();
        Assertions.assertFalse(dataSourceExpressions.hasDataSourceExpression());
        Assertions.assertTrue(datasSourceExpressions.isEmpty());
    }

    @Test
    public void testIndex() {
        ValueExpressionImpl indexExpression = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        AndExpression ast = new AndExpression(
                indexExpression,
                new ValueExpressionImpl("10000", "equals", Expression.Tag.LATEST)
        );
        DataSourceExpressions dataSourceExpressions = new DataSourceExpressions(ast);
        List<Expression> datasSourceExpressions = dataSourceExpressions.dataSourceExpressions();
        Assertions.assertTrue(dataSourceExpressions.hasDataSourceExpression());
        Assertions.assertEquals(1, datasSourceExpressions.size());
        Assertions.assertEquals(indexExpression, datasSourceExpressions.get(0));
    }

    @Test
    public void testSourcetype() {
        ValueExpressionImpl sourceTypeExpression = new ValueExpressionImpl("test", "equals", Expression.Tag.SOURCETYPE);
        AndExpression ast = new AndExpression(
                sourceTypeExpression,
                new ValueExpressionImpl("10000", "equals", Expression.Tag.LATEST)
        );
        DataSourceExpressions dataSourceExpressions = new DataSourceExpressions(ast);
        List<Expression> datasSourceExpressions = dataSourceExpressions.dataSourceExpressions();
        Assertions.assertTrue(dataSourceExpressions.hasDataSourceExpression());
        Assertions.assertEquals(1, datasSourceExpressions.size());
        Assertions.assertEquals(sourceTypeExpression, datasSourceExpressions.get(0));
    }

    @Test
    public void testTwoDataSourceExpressions() {
        ValueExpressionImpl indexExpression = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        ValueExpressionImpl sourceTypeExpression = new ValueExpressionImpl("test", "equals", Expression.Tag.SOURCETYPE);

        AndExpression ast = new AndExpression(
                indexExpression,
                sourceTypeExpression
        );
        DataSourceExpressions dataSourceExpressions = new DataSourceExpressions(ast);
        List<Expression> datasSourceExpressions = dataSourceExpressions.dataSourceExpressions();
        Assertions.assertTrue(dataSourceExpressions.hasDataSourceExpression());
        Assertions.assertEquals(2, datasSourceExpressions.size());
        Assertions.assertEquals(indexExpression, datasSourceExpressions.get(0));
        Assertions.assertEquals(sourceTypeExpression, datasSourceExpressions.get(1));
    }
}