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
import org.teiid.language.Join.JoinType;
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
import org.teiid.translator.document.DocumentNode;

public class IckleConvertionVisitor extends SQLStringVisitor {
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected RuntimeMetadata metadata;
    protected List<Expression> projectedExpressions = new ArrayList<>();
    protected NamedTable namedTable;
    protected NamedTable originalNamedTable;
    private Integer rowLimit;
    private Integer rowOffset;
    private boolean includePK;
    private boolean avoidProjection = false;
    private InfinispanDocumentNode rootNode;
    private DocumentNode joinedNode;
    private List<String> projectedDocumentAttributes = new ArrayList<>();

    public IckleConvertionVisitor(RuntimeMetadata metadata, boolean includePK) {
        this.metadata = metadata;
        this.includePK = includePK;
        this.shortNameOnly = true;
    }

    public Table getTable() {
        return namedTable.getMetadataObject();
    }

    @Override
    public void visit(NamedTable obj) {
        this.originalNamedTable = obj;

        if (this.includePK) {
            KeyRecord pk = obj.getMetadataObject().getPrimaryKey();
            if (pk != null) {
                for (Column column : pk.getColumns()) {
                    projectedExpressions.add(new ColumnReference(obj, column.getName(), column, column.getJavaType()));
                }
            }
        }

        String messageName = null;
        String mergedTableName = ProtobufMetadataProcessor.getMerge(obj.getMetadataObject());
        if (mergedTableName == null) {
            messageName = getMessageName(obj.getMetadataObject());
            this.namedTable = obj;
            if (rootNode == null) {
                this.rootNode = new InfinispanDocumentNode(obj.getMetadataObject(), true);
                this.joinedNode = this.rootNode;
            }
        } else {
            try {
                Table mergedTable = this.metadata.getTable(mergedTableName);
                messageName = getMessageName(mergedTable);
                this.namedTable = new NamedTable(mergedTable.getName(), obj.getCorrelationName(), mergedTable);
                if (rootNode == null) {
                    this.rootNode = new InfinispanDocumentNode(mergedTable, true);
                    this.joinedNode = this.rootNode.joinWith(JoinType.INNER_JOIN,
                            new InfinispanDocumentNode(obj.getMetadataObject(), true));
                }
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }

        buffer.append(messageName);
        if (obj.getCorrelationName() != null) {
            buffer.append(Tokens.SPACE);
            buffer.append(obj.getCorrelationName());
        }
    }

    private String getMessageName(Table obj) {
        String messageName;
        messageName = ProtobufMetadataProcessor.getMessageName(obj);
        if (messageName == null) {
            messageName = obj.getName();
        }
        return messageName;
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
    public void visit(ColumnReference obj) {
        buffer.append(getQualifiedName(obj.getMetadataObject()));
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
                if (column.getParent().equals(this.namedTable.getMetadataObject())){
                    this.projectedExpressions.add(new ColumnReference(this.namedTable, column.getName(), column, column.getJavaType()));
                } else {
                    this.projectedExpressions.add(new ColumnReference(this.originalNamedTable, column.getName(), column, column.getJavaType()));
                }
            }
            boolean nested = false;
            if (ProtobufMetadataProcessor.getMessageName(column) != null
                    || ProtobufMetadataProcessor.getMerge((Table) column.getParent()) != null) {
                this.avoidProjection = true;
                nested = true;
            }
            try {
                this.projectedDocumentAttributes.add(MarshallerBuilder.getDocumentAttributeName(column, nested, this.metadata));
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else if (obj.getExpression() instanceof Function) {
            if (this.namedTable.equals(this.originalNamedTable)) {
                this.exceptions.add(new TranslatorException(InfinispanPlugin.Event.TEIID25008, InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25008)));
            }
            AggregateFunction func = (AggregateFunction)obj.getExpression();
            this.projectedExpressions.add(func);
            // Aggregate functions can not be part of the implicit query projection when the complex object is involved
            // thus not adding to projectedDocumentAttributes. i.e. sum(g2.g3.e1) is not supported by Infinispan AFAIK.
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
                return pseudoColumnParent.getColumnByName(getName(column));
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        return column;
    }

    public String getQuery(boolean selectAllColumns) {
        StringBuilder sb = new StringBuilder();
        if (!this.avoidProjection) {
            addSelectedColumns(selectAllColumns, sb);
            sb.append(Tokens.SPACE);
        }
        sb.append(super.toString());
        return  sb.toString();
    }

    String getQualifiedName(Column column) {
        String aliasName = this.namedTable.getCorrelationName();
        String nis = getName(column);
        String parentName = ProtobufMetadataProcessor.getParentColumnName(column);
        if (parentName == null && !ProtobufMetadataProcessor.isPseudo(column)) {
            parentName = ProtobufMetadataProcessor.getParentColumnName((Table)column.getParent());
        }
        if (parentName != null) {
            nis = parentName + Tokens.DOT + nis;
        }
        if (aliasName != null) {
            return aliasName + Tokens.DOT + nis;
        }
        return nis;
    }

    StringBuilder addSelectedColumns(boolean selectAllColumns, StringBuilder sb) {
        sb.append(SQLConstants.Reserved.SELECT).append(Tokens.SPACE);

        if (selectAllColumns) {
            boolean first = true;
            for (Column column : getTable().getColumns()) {
                if (column.isSelectable()) {
                    String nis = getQualifiedName(column);
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
                String nis = getQualifiedName(column);
                sb.append(nis);
            } else if (expr instanceof Function) {
                Function func = (Function) expr;
                sb.append(func.getName()).append(Tokens.LPAREN);
                if (func.getParameters().isEmpty() && SQLConstants.NonReserved.COUNT.equalsIgnoreCase(func.getName())) {
                    sb.append(Tokens.ALL_COLS);
                } else {
                    ColumnReference columnRef = (ColumnReference) func.getParameters().get(0);
                    Column column = columnRef.getMetadataObject();
                    String nis = getQualifiedName(column);
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

    @Override
    protected boolean useAsInGroupAlias(){
        return false;
    }

    public List<String> getProjectedDocumentAttributes() throws TranslatorException {
        return projectedDocumentAttributes;
    }

    RuntimeMetadata getMetadata() {
        return metadata;
    }

    InfinispanDocumentNode getDocumentNode() {
        return this.rootNode;
    }
}
