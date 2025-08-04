package com.teragrep.pth_06.ast.transform;

import com.teragrep.pth_06.ast.EmptyExpression;
import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.xml.AndExpression;
import com.teragrep.pth_06.ast.xml.ValueExpressionImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class EmptyPrunedExpressionTest {

    @Test
    public void testEmptyBinaryValueFlattened() {
        ValueExpressionImpl value = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        Expression and = new AndExpression(new EmptyExpression(),  value);
        Expression result = new EmptyPrunedExpression(and).transformedExpression();
        Assertions.assertEquals(value, result);
    }

    @Test
    public void testNoEmptyValuesPassthrough() {
        ValueExpressionImpl value = new ValueExpressionImpl("test", "equals", Expression.Tag.INDEX);
        Expression and = new AndExpression(value,  value);
        Expression result = new EmptyPrunedExpression(and).transformedExpression();
        Assertions.assertEquals(and, result);
    }
}