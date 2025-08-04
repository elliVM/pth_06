package com.teragrep.pth_06.ast.transform;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.RowKeyExpression;
import com.teragrep.pth_06.ast.RowKeyExpressionImpl;
import com.teragrep.pth_06.ast.ValueExpression;
import com.teragrep.pth_06.ast.meta.StreamMetadata;
import com.teragrep.pth_06.planner.walker.conditions.HostCondition;
import com.teragrep.pth_06.planner.walker.conditions.IndexCondition;
import com.teragrep.pth_06.planner.walker.conditions.SourceTypeCondition;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

// transforms AND node into hbase row keys
public class AndNodeToRowKeys {
    private final Logger LOGGER = LoggerFactory.getLogger(AndNodeToRowKeys.class);
    private final Expression origin;
    private final DSLContext ctx;

    AndNodeToRowKeys(Expression origin, DSLContext ctx) {
        this.origin = origin;
        this.ctx =ctx;
    }

    public List<RowKeyExpression> rowKeyExpressions() {
        if (origin.tag() != Expression.Tag.AND) {
            throw new UnsupportedOperationException("Unsupported tag <" + origin.tag() +">. only AND supported");
        }
        List<Expression> children = origin.children();
        List<ValueExpression> earliestList = new ArrayList<>();
        List<ValueExpression> latestList = new ArrayList<>();
        Condition condition = DSL.noCondition();

        // build a condition to select stream ids
        for (Expression child: children) {
            Expression.Tag tag = child.tag();
            switch (tag) {
                case INDEX:
                    ValueExpression indexValueExpression = (ValueExpression) child;
                    String indexValue = indexValueExpression.value();
                    String indexOperation = indexValueExpression.operation();
                    Condition indexCondition = new IndexCondition(indexValue, indexOperation, true).condition();
                    condition = condition.and(indexCondition);
                    break;
                case SOURCETYPE:
                    ValueExpression sourceTypeExpression = (ValueExpression) child;
                    String sourceTypeValue = sourceTypeExpression.value();
                    String sourceTypeOperation = sourceTypeExpression.operation();
                    Condition sourceTypeCondition = new SourceTypeCondition(sourceTypeValue, sourceTypeOperation, true).condition();
                    condition = condition.and(sourceTypeCondition);
                    break;
                case HOST:
                    ValueExpression hostValueExpression = (ValueExpression) child;
                    String hostValue = hostValueExpression.value();
                    String hostOperation = hostValueExpression.operation();
                    Condition hostCondition = new HostCondition(hostValue, hostOperation, true).condition();
                    condition = condition.and(hostCondition);
                    break;
                case EARLIEST:
                    ValueExpression earliestValue = (ValueExpression) child;
                    earliestList.add(earliestValue);
                    break;
                case LATEST:
                    ValueExpression latestValue = (ValueExpression) child;
                    latestList.add(latestValue);
                case OR:
                    LOGGER.info("Passthrough OR tag");
                case AND:
                    throw new UnsupportedOperationException("AND node should not have AND nodes as children");
                default:
                    throw new IllegalArgumentException("Unsupported tag <" + tag + ">");
            }
        }

        // add a wildcard index condition if missing
        if (condition.equals(DSL.noCondition())) {
            LOGGER.info("No directory set using wildcard index='*' for all data sources");
            condition = new IndexCondition("*", "EQUALS", true).condition();
        }

        // find stream ids that match condition
        StreamMetadata streamMetadata = new StreamMetadata(ctx, condition);
        List<Long> streamIds = streamMetadata.streamIdList();
        Long earliest = Long.valueOf(earliestList.get(0).value());
        Long latest = Long.valueOf(latestList.get(0).value());

        // create row key for each stream id
        List<RowKeyExpression> rowKeyExpressions = new ArrayList<>();
        for(Long streamId: streamIds) {
            RowKeyExpression rowKeyExpressionImpl = new RowKeyExpressionImpl(streamId, earliest, latest);
            rowKeyExpressions.add(rowKeyExpressionImpl);
        }
        return rowKeyExpressions;
    }
}
