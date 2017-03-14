/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.infinispan.hotrod;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionContextQueryBuilder;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.impl.PathExpression;
import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class DSLConvertionVisitor extends HierarchyVisitor {
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected RuntimeMetadata metadata;
    protected Table table;
    protected List<Expression> projectedExpressions = new ArrayList<>();
    protected Integer limit;
    protected Integer offset;
    protected Stack<Object> onGoingExpression  = new Stack<Object>();
    protected QueryFactory queryFactory;
    protected QueryBuilder queryBuilder;

    public DSLConvertionVisitor(RuntimeMetadata metadata, QueryFactory queryFactory, QueryBuilder queryBuilder) {
        this.metadata = metadata;
        this.queryFactory = queryFactory;
        this.queryBuilder = queryBuilder;
        this.onGoingExpression.push(queryBuilder);
    }

    public Table getTable() {
        return table;
    }

    public void append(LanguageObject obj) {
        if (obj != null) {
            visitNode(obj);
        }
    }

    // only to be used in update situation
    List<String> getProjectedColumnNames() {
        ArrayList<String> names = new ArrayList<>();
        for (Expression expr : projectedExpressions) {
            PathExpression pathExpr = (PathExpression)expr;
            names.add(pathExpr.getPath());
        }
        return names;
    }

    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                append(items.get(i));
            }
        }
    }

    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            append(items[0]);
            for (int i = 1; i < items.length; i++) {
                append(items[i]);
            }
        }
    }

    @Override
    public void visit(NamedTable obj) {
        this.table = obj.getMetadataObject();
        // always add PK columns in every query if one exists
        KeyRecord pk = this.table.getPrimaryKey();
        if (pk != null) {
            for (Column column : pk.getColumns()) {
                if (!isProjected(column.getName())) {
                    this.projectedExpressions.add(new PathExpression(null,  column.getName()));
                }
            }
        }
    }

    boolean isProjected(String columnName) {
        for (Expression expr:this.projectedExpressions) {
            PathExpression path = (PathExpression)expr;
            if (columnName.equals(path.getPath())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void visit(ColumnReference obj) {
        Column column = obj.getMetadataObject();
        if (!column.isSelectable()) {
            this.exceptions.add(new TranslatorException(InfinispanPlugin.Util
                    .gs(InfinispanPlugin.Event.TEIID25001, column.getName())));
        }
        column = normalizePseudoColumn(column);
        PathExpression expression = new PathExpression(null, obj.getMetadataObject().getName());
        this.onGoingExpression.push(expression);
    }

    @Override
    public void visit(Literal obj) {
        onGoingExpression.add(obj.getValue());
    }
/*
    @Override
    public void visit(Join obj) {
        // joins are not used currently
        if (obj.getLeftItem() instanceof Join) {
            Condition updated = obj.getCondition();
            append(obj.getLeftItem());
            Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
            try {
                updated = this.odataQuery.addNavigation(obj.getCondition(), obj.getJoinType(), right);
                obj.setCondition(updated);
                if (updated != null) {
                    this.conditionFragments.add(obj.getCondition());
                }
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else if (obj.getRightItem() instanceof Join) {
            Condition updated = obj.getCondition();
            append(obj.getRightItem());
            Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
            try {
                updated = this.odataQuery.addNavigation(obj.getCondition(), obj.getJoinType(), left);
                obj.setCondition(updated);
                if (updated != null) {
                    this.conditionFragments.add(obj.getCondition());
                }
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else {
            Condition updated = obj.getCondition();
            Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
            Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
            try {
                if (ODataMetadataProcessor.isComplexType(left) ||
                        ODataMetadataProcessor.isNavigationType(left)) {
                    throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17027, left.getName()));
                }
                updated = this.odataQuery.addNavigation(obj.getCondition(), obj.getJoinType(), left, right);
                obj.setCondition(updated);
                if (updated != null) {
                    this.conditionFragments.add(obj.getCondition());
                }
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
    }
*/

    @Override
    public void visit(Limit obj) {
        if (obj.getRowOffset() != 0) {
            this.offset = new Integer(obj.getRowOffset());
        }
        if (obj.getRowLimit() != 0) {
            this.limit = new Integer(obj.getRowLimit());
        }
    }

    @Override
    public void visit(Select obj) {
        visitNodes(obj.getDerivedColumns());
        visitNodes(obj.getFrom());
        visitNode(obj.getWhere());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
    }

    @Override
    public void visit(DerivedColumn obj) {
        append(obj.getExpression());
        this.projectedExpressions.add((Expression)this.onGoingExpression.pop());
    }

    Column normalizePseudoColumn(Column column) {
        String pseudo = ProtobufMetadataProcessor.getPseudo(column);
        if (pseudo != null) {
            try {
                Table columnParent = (Table)column.getParent();
                Table pseudoColumnParent = this.metadata.getTable(
                        ProtobufMetadataProcessor.getMerge(columnParent));
                return pseudoColumnParent.getColumnByName(pseudo);
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        return column;
    }

    public QueryBuilder getTargetQueryBuilder(boolean selectAllColumns) {
        List<Expression> select = addSelectedColumns(selectAllColumns);
        queryBuilder.select(select.toArray(new Expression[select.size()]));
        return  queryBuilder;
    }

    List<Expression> addSelectedColumns(boolean selectAllColumns) {
        if (selectAllColumns) {
            List<Expression> expressions = new ArrayList<>();
            for (Column column : this.table.getColumns()) {
                if (column.isSelectable()) {
                    String nis = (column.getNameInSource() == null)?column.getName():column.getNameInSource();
                    expressions.add(new PathExpression(null,  nis));
                }
            }
            this.projectedExpressions = expressions;
            return expressions;
        }
        return this.projectedExpressions;
    }

    @Override
    public void visit(AggregateFunction obj) {
        if (!obj.getParameters().isEmpty()) {
            append(obj.getParameters());
        }

        PathExpression path = (PathExpression)this.onGoingExpression.pop();

        Expression expr = null;
        if (obj.getName().equals(AggregateFunction.COUNT)) {
            expr = new PathExpression(PathExpression.AggregationType.COUNT, path.getPath());
        }
        else if (obj.getName().equals(AggregateFunction.AVG)) {
            expr = new PathExpression(PathExpression.AggregationType.AVG, path.getPath());
        }
        else if (obj.getName().equals(AggregateFunction.SUM)) {
            expr = new PathExpression(PathExpression.AggregationType.SUM, path.getPath());
        }
        else if (obj.getName().equals(AggregateFunction.MIN)) {
            new PathExpression(PathExpression.AggregationType.MIN, path.getPath());
        }
        else if (obj.getName().equals(AggregateFunction.MAX)) {
            new PathExpression(PathExpression.AggregationType.MAX, path.getPath());
        }
        else {
            this.exceptions.add(new TranslatorException(
                    InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25007, obj.getName())));
        }

        if (expr != null) {
            this.onGoingExpression.push(expr);
        }
    }

    @Override
    public void visit(Comparison obj) {
        if (!this.onGoingExpression.isEmpty()) {
            FilterConditionBeginContext begin = (FilterConditionBeginContext)onGoingExpression.pop();

            append(obj.getLeftExpression());
            Expression leftExpr = (Expression)this.onGoingExpression.pop();

            append(obj.getRightExpression());
            Object rightExpr = this.onGoingExpression.pop();

            FilterConditionContextQueryBuilder next = null;

            switch(obj.getOperator()) {
            case EQ:
                next = begin.having(leftExpr).eq(rightExpr);
                break;
            case NE:
                next = begin.not().having(leftExpr).eq(rightExpr);
                break;
            case LT:
                next = begin.having(leftExpr).lt(rightExpr);
                break;
            case LE:
                next = begin.having(leftExpr).lte(rightExpr);
                break;
            case GT:
                next = begin.having(leftExpr).gt(rightExpr);
                break;
            case GE:
                next = begin.having(leftExpr).gte(rightExpr);
                break;
            }

            this.onGoingExpression.push(next);
        } else {
            append(obj.getLeftExpression());
            Expression leftExpr = (Expression)this.onGoingExpression.pop();

            append(obj.getRightExpression());
            Object rightExpr = this.onGoingExpression.pop();

            FilterConditionContextQueryBuilder next = null;

            switch(obj.getOperator()) {
            case EQ:
                next = queryFactory.having(leftExpr).eq(rightExpr);
                break;
            case NE:
                next = queryFactory.not().having(leftExpr).eq(rightExpr);
                break;
            case LT:
                next = queryFactory.having(leftExpr).lt(rightExpr);
                break;
            case LE:
                next = queryFactory.having(leftExpr).lte(rightExpr);
                break;
            case GT:
                next = queryFactory.having(leftExpr).gt(rightExpr);
                break;
            case GE:
                next = queryFactory.having(leftExpr).gte(rightExpr);
                break;
            }
            this.onGoingExpression.push(next);
        }
    }

    @Override
    public void visit(AndOr obj) {
        append(obj.getLeftCondition());

        FilterConditionContext left = (FilterConditionContext)this.onGoingExpression.pop();

        append(obj.getRightCondition());
        FilterConditionContext right = (FilterConditionContext)this.onGoingExpression.pop();

        FilterConditionBeginContext next = null;
        switch(obj.getOperator()) {
        case AND:
            next = left.and(right);
            break;
        case OR:
            next = left.or(right);
            break;
        }
        this.onGoingExpression.push(next);
    }

    @Override
    public void visit(Function obj) {
        this.exceptions.add(new TranslatorException("Function not supported:"+obj.getName()));
    }

    @Override
    public void visit(GroupBy obj) {
        append(obj.getElements());
        String[] paths = new String[obj.getElements().size()];
        for (int i = 0; i < obj.getElements().size(); i++) {
            PathExpression expr = (PathExpression)this.onGoingExpression.pop();
            paths[i] = expr.getPath();
        }
        queryBuilder.groupBy(paths);
    }

    @Override
    public void visit(In obj) {
        append(obj.getLeftExpression());
        Expression leftExpr = (Expression)this.onGoingExpression.pop();

        FilterConditionEndContext end = null;
        if (obj.isNegated()) {
            end = queryFactory.having(leftExpr);
            FilterConditionContextQueryBuilder qb = end.in(buildInValues(obj));
            this.queryBuilder.not(qb);
        } else {
            end = queryBuilder.having(leftExpr);
            end.in(buildInValues(obj));
        }
    }

    protected List<Object> buildInValues(In obj) {
        append(obj.getRightExpressions());
        ArrayList<Object> values = new ArrayList<>();
        for (int i = 0; i < obj.getRightExpressions().size(); i++) {
            Object rightExpr = this.onGoingExpression.pop();
            values.add(0, rightExpr);
        }
        return values;
    }

    @Override
    public void visit(IsNull obj) {
        append(obj.getExpression());
        Expression leftExpr = (Expression)this.onGoingExpression.pop();

        if (obj.isNegated()) {
            FilterConditionContextQueryBuilder qb = queryFactory.having(leftExpr).isNull();
            this.queryBuilder.not(qb);
        } else {
            queryBuilder.having(leftExpr).isNull();
        }
    }

    @Override
    public void visit(Like obj) {
        append(obj.getLeftExpression());
        Expression leftExpr = (Expression)this.onGoingExpression.pop();

        append(obj.getRightExpression());
        String rightExpr = (String) this.onGoingExpression.pop();

        if (obj.isNegated()) {
            FilterConditionContextQueryBuilder qb = queryFactory.having(leftExpr).like(rightExpr);
            this.queryBuilder.not(qb);
        } else {
            queryBuilder.having(leftExpr).like(rightExpr);
        }
    }

    @Override
    public void visit(OrderBy obj) {
        append(obj.getSortSpecifications());
    }

    @Override
    public void visit(SortSpecification obj) {
        append(obj.getExpression());
        Expression expr = (Expression)this.onGoingExpression.pop();
        queryBuilder.orderBy(expr, obj.getOrdering()==SortSpecification.Ordering.ASC?SortOrder.ASC:SortOrder.DESC);
    }
}
