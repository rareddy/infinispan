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

import static org.teiid.language.SQLConstants.Reserved.HAVING;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Limit;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class IckleConvertionVisitor extends SQLStringVisitor {
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected RuntimeMetadata metadata;
    protected List<Expression> projectedExpressions = new ArrayList<>();
    protected NamedTable table;
    private Integer rowLimit;
    private Integer rowOffset;
    private boolean includePK;

    public IckleConvertionVisitor(RuntimeMetadata metadata, boolean includePK) {
        this.metadata = metadata;
        this.shortNameOnly = true;
        this.includePK = includePK;
    }

    public Table getTable() {
        return table.getMetadataObject();
    }

    @Override
    public void visit(NamedTable obj) {
        this.table = obj;

        if (this.includePK) {
            KeyRecord pk = getTable().getPrimaryKey();
            if (pk != null) {
                for (Column column : pk.getColumns()) {
                    projectedExpressions.add(new ColumnReference(obj, column.getName(), column, column.getJavaType()));
                }
            }
        }

        if(this.table != null) {
            buffer.append(ProtobufMetadataProcessor.getMessageName(getTable()));
        } else {
            buffer.append(obj.getName());
        }
    }

    public boolean isPartOfPrimaryKey(String columnName) {
        KeyRecord pk = getTable().getPrimaryKey();
        if (pk != null) {
            for (Column column:pk.getColumns()) {
                if (column.getName().equals(columnName)) {
                    return true;
                }
            }
        }
        return false;
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
            this.rowOffset = new Integer(obj.getRowOffset());
        }
        if (obj.getRowLimit() != 0) {
            this.rowLimit = new Integer(obj.getRowLimit());
        }
    }


    @Override
    public void visit(Select obj) {
        buffer.append(SQLConstants.Reserved.FROM).append(Tokens.SPACE);
        visitNodes(obj.getFrom());

        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE);
            buffer.append(SQLConstants.Reserved.WHERE).append(Tokens.SPACE);
            visitNode(obj.getWhere());
        }

        if (obj.getGroupBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getGroupBy());
        }

        if (obj.getHaving() != null) {
            buffer.append(Tokens.SPACE)
                  .append(HAVING)
                  .append(Tokens.SPACE);
            append(obj.getHaving());
        }

        if (obj.getOrderBy() != null) {
            buffer.append(Tokens.SPACE);
            visitNode(obj.getOrderBy());
        }

        if (obj.getLimit() != null) {
            buffer.append(Tokens.SPACE);
            visitNode(obj.getLimit());
        }

        visitNodes(obj.getDerivedColumns());
    }

    @Override
    public void visit(DerivedColumn obj) {
        if (obj.getExpression() instanceof ColumnReference) {
            Column column = ((ColumnReference)obj.getExpression()).getMetadataObject();
            if (!column.isSelectable()) {
                this.exceptions.add(new TranslatorException(InfinispanPlugin.Util
                        .gs(InfinispanPlugin.Event.TEIID25001, column.getName())));
            }
            column = normalizePseudoColumn(column);
            if (!this.includePK || !isPartOfPrimaryKey(column.getName())) {
                this.projectedExpressions.add(new ColumnReference(this.table, column.getName(), column, column.getJavaType()));
            }
        }
        else if (obj.getExpression() instanceof AggregateFunction) {
            AggregateFunction func = (AggregateFunction)obj.getExpression();
            this.projectedExpressions.add(func);
        }
        else if (obj.getExpression() instanceof Function) {
            Function func = (Function)obj.getExpression();
            this.projectedExpressions.add(func);
        }
        else {
            this.exceptions.add(new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25002, obj)));
        }
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

    public String getQuery(boolean selectAllColumns) {
        StringBuilder sb = new StringBuilder();
        addSelectedColumns(selectAllColumns, sb);

        sb.append(Tokens.SPACE).append(super.toString());
        return  sb.toString();
    }

    StringBuilder addSelectedColumns(boolean selectAllColumns, StringBuilder sb) {
        sb.append(SQLConstants.Reserved.SELECT).append(Tokens.SPACE);

        if (selectAllColumns) {
            boolean first = true;
            for (Column column : getTable().getColumns()) {
                if (column.isSelectable()) {
                    String nis = (column.getNameInSource() == null)?column.getName():column.getNameInSource();
                    if (!first) {
                        sb.append(Tokens.COMMA).append(Tokens.SPACE);
                    }
                    sb.append(nis);
                    first = false;
                }
            }
            return sb;
        }


        boolean first = true;
        for (Expression expr : this.projectedExpressions) {

            if (!first) {
                sb.append(Tokens.COMMA).append(Tokens.SPACE);
            }

            if (expr instanceof ColumnReference) {
                Column column = ((ColumnReference) expr).getMetadataObject();
                String nis = (column.getNameInSource() == null) ? column.getName() : column.getNameInSource();
                sb.append(nis);
            } else if (expr instanceof Function) {
                Function func = (Function) expr;
                sb.append(func.getName()).append(Tokens.LPAREN);
                if (func.getParameters().isEmpty() && SQLConstants.NonReserved.COUNT.equalsIgnoreCase(func.getName())) {
                    sb.append(Tokens.ALL_COLS);
                } else {
                    ColumnReference columnRef = (ColumnReference) func.getParameters().get(0);
                    Column column = columnRef.getMetadataObject();
                    String nis = (column.getNameInSource() == null) ? column.getName() : column.getNameInSource();
                    sb.append(nis);
                }
                sb.append(Tokens.RPAREN);
            }
            first = false;
        }
        return sb;
    }

    public Integer getRowLimit() {
        return rowLimit;
    }

    public Integer getRowOffset() {
        return rowOffset;
    }
}
