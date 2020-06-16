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

import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.SymbolReference;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.AND;
import static java.util.Objects.requireNonNull;

public final class FilterNullQueryUtil
{
    public static boolean containsNullFilterFor(PlanNode node, VariableReferenceExpression needle, Lookup lookup)
    {
        return node.accept(new SearchNullPredicatePlanVisitor(needle, lookup), null);
    }

    private static final class SearchNullPredicatePlanVisitor
            extends InternalPlanVisitor<Boolean, Void>
    {
        private final VariableReferenceExpression needle;
        private final Lookup lookup;

        private SearchNullPredicatePlanVisitor(VariableReferenceExpression needle, Lookup lookup)
        {
            this.needle = needle;
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        public Boolean visitPlan(PlanNode node, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        @Override
        public Boolean visitFilter(FilterNode node, Void context)
        {
            RowExpression rowExpression = node.getPredicate();
            if (isExpression(rowExpression)) {
                Expression expression = castToExpression(rowExpression);
                return searchNullPredicate(expression);
            }
            return false;
        }

        private boolean searchNullPredicate(Expression expression)
        {
            if (expression instanceof IsNotNullPredicate) {
                Expression nullPredicate = ((IsNotNullPredicate) expression).getValue();
                if (nullPredicate instanceof SymbolReference) {
                    String name = ((SymbolReference) nullPredicate).getName();
                    return name.equals(needle.getName());
                }
            } else if (expression instanceof LogicalBinaryExpression) {
                LogicalBinaryExpression lbe = (LogicalBinaryExpression) expression;
                if (lbe.getOperator().equals(AND)) {
                    return searchNullPredicate(lbe.getLeft()) || searchNullPredicate(lbe.getRight());
                }
            }
            return false;
        }
    }
}
