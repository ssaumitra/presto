package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;

public class RemoveNullsInnerJoin implements Rule<JoinNode> {

    private static final Pattern<JoinNode> PATTERN = join().matching(
            node -> node.getType() == JoinNode.Type.INNER);

    @Override
    public Pattern<JoinNode> getPattern() {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session) {
        return true;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context) {
        // TODO: Improve this IF
        if(node.getFilter().isPresent())    {
            return Result.empty();
        }
        List<JoinNode.EquiJoinClause> criterions = node.getCriteria();
        LinkedList<Expression> updatedFilters = new LinkedList<>();
        node.getFilter().map(OriginalExpressionUtils::castToExpression).ifPresent(updatedFilters::add);
        for(JoinNode.EquiJoinClause criteria: criterions)    {
            updatedFilters.add(new IsNotNullPredicate(castToExpression(criteria.getLeft())));
            updatedFilters.add(new IsNotNullPredicate(castToExpression(criteria.getRight())));
        }
        RowExpression updatedFilterExpression = castToRowExpression(ExpressionUtils.and(updatedFilters));
        return Result.ofPlanNode(
                new JoinNode(node.getId(),
                node.getType(),
                node.getLeft(),
                node.getRight(),
                node.getCriteria(),
                node.getOutputVariables(),
                Optional.of(updatedFilterExpression),
                node.getLeftHashVariable(),
                node.getRightHashVariable(),
                node.getDistributionType()));
    }
}
