/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.iterative.rule.FilterNullQueryUtil.containsNullFilterFor;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.Join.type;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.sources;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;

/**
 * Transforms:
 * <pre>
 *    - Join
 *       - left source
 *       - right source
 * </pre>
 * Into:
 * <pre>
 *    - Join
 *       - Filter [join key not null] (if Join is right or inner)
 *          - left source
 *       - Filter [join key not null] (if Join is right or inner)
 *          - right source
 * </pre>
 */

public class FilterNullsInJoin
        implements Rule<JoinNode>
{
    private static final Capture<List<PlanNode>> CHILDREN = newCapture();
    private static final Pattern<JoinNode> PATTERN = join()
            .with(type().matching(type -> type == LEFT || type == RIGHT || type == INNER))
            .with(sources().capturedAs(CHILDREN));

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();
        List<EquiJoinClause> joinCriteria = node.getCriteria();
        if (node.getType() == RIGHT || node.getType() == INNER) {
            left = buildNullFilter(left, context, joinCriteria, EquiJoinClause::getLeft);
        }
        if (node.getType() == LEFT || node.getType() == INNER) {
            right = buildNullFilter(right, context, joinCriteria, EquiJoinClause::getRight);
        }
        if (node.getLeft() != left || node.getRight() != right) {
            return Result.ofPlanNode(
                    new JoinNode(
                            node.getId(),
                            node.getType(),
                            left,
                            right,
                            node.getCriteria(),
                            node.getOutputVariables(),
                            node.getFilter(),
                            node.getLeftHashVariable(),
                            node.getRightHashVariable(),
                            node.getDistributionType()));
        }
        return Result.empty();
    }

    private PlanNode buildNullFilter(PlanNode node, Context context,
                                     List<EquiJoinClause> joinCriteria,
                                     Function<EquiJoinClause, VariableReferenceExpression> criteriaExtractor)
    {

        Optional<Expression> nullFilter = joinCriteria
                .stream()
                .map(criteriaExtractor)
                .filter(e -> !containsNullFilterFor(node, e, context.getLookup()))
                .map(vre -> (Expression) new IsNotNullPredicate(new SymbolReference(vre.getName())))
                .reduce(QueryUtil::logicalAnd);

        if (nullFilter.isPresent()) {
            return new FilterNode(context.getIdAllocator().getNextId(), node, castToRowExpression(nullFilter.get()));
        }
        return node;
    }
}
