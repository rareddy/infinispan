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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.infinispan.api.InfinispanDocument;
import org.teiid.infinispan.api.TableWireFormat;
import org.teiid.language.ColumnReference;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Function;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants;
import org.teiid.language.Update;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class InfinispanUpdateVisitor extends IckleConvertionVisitor {
    protected enum OperationType {INSERT, UPDATE, DELETE, UPSERT};
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private OperationType operationType;
    private InfinispanDocument insertPayload;
    private Map<String, Object> updatePayload = new HashMap<>();
    private Object identity;
    private boolean nested;

    public InfinispanUpdateVisitor(RuntimeMetadata metadata) {
        super(metadata, true);
    }

    public Object getIdentity() {
        return identity;
    }

    public OperationType getOperationType() {
        return this.operationType;
    }


    public InfinispanDocument getInsertPayload() {
        return insertPayload;
    }

    public Map<String, Object> getUpdatePayload() {
        return updatePayload;
    }

    public boolean isNestedOperation() {
        return this.nested;
    }

    @Override
    public void visit(Insert obj) {
        this.operationType = OperationType.INSERT;
        if (obj.isUpsert()) {
            this.operationType = OperationType.UPSERT;
        }
        visitNode(obj.getTable());

        Column pkColumn = getPrimaryKey();

        // table that insert issued for
        Table table = obj.getTable().getMetadataObject();
        try {
            // create the top table parent document, where insert is actually being done at
            InfinispanDocument targetDocument = buildTargetDocument(table);

            // build the payload object from insert
            int elementCount = obj.getColumns().size();
            for (int i = 0; i < elementCount; i++) {

                ColumnReference columnReferce = obj.getColumns().get(i);
                Column column = columnReferce.getMetadataObject();
                this.projectedExpressions.add(columnReferce);

                List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();
                Expression expr = values.get(i);
                Object value = resolveExpressionValue(expr);

                updateDocument(targetDocument, column, value);

                if ((pkColumn != null && column.equals(pkColumn) || pkColumn.equals(normalizePseudoColumn(column)))) {
                    this.identity = value;
                }
            }

            // add default values in for the table insert issued for
            for (Column column : table.getColumns()) {
                String attrName = MarshallerBuilder.getDocumentAttributeName(column, this.nested, this.metadata);
                if (column.getDefaultValue() != null && !targetDocument.getProperties().containsKey(attrName)) {
                    updateDocument(targetDocument, column, column.getDefaultValue());
                }
            }
            this.insertPayload = targetDocument;
        } catch (NumberFormatException e) {
            this.exceptions.add(new TranslatorException(e));
        } catch (TranslatorException e) {
            this.exceptions.add(new TranslatorException(e));
        }

        if (this.identity == null) {
            this.exceptions.add(new TranslatorException(
                    InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25004, getTopLevelTable().getName())));
        }
    }

    @SuppressWarnings("unchecked")
    private void updateDocument(InfinispanDocument targetDocument, Column column, Object value)
            throws TranslatorException {
        String attrName = MarshallerBuilder.getDocumentAttributeName(column, this.nested, this.metadata);
        if (value instanceof List) {
            List<Object> l = (List<Object>)value;
            for(Object o : l) {
                targetDocument.addArrayProperty(attrName, o);
            }
        } else {
            targetDocument.addProperty(attrName, value);
        }
        this.updatePayload.put(attrName, value);
    }

    private InfinispanDocument buildTargetDocument(Table table) throws TranslatorException {
        TreeMap<Integer, TableWireFormat> wireMap = MarshallerBuilder.getWireMap(getTopLevelTable(), metadata);
        String messageName = ProtobufMetadataProcessor.getMessageName(table);
        if (table.equals(getTopLevelTable())) {
            return new InfinispanDocument(messageName, wireMap, null);
        } else {
            // now create the document at child node
            int parentTag = ProtobufMetadataProcessor.getParentTag(table);
            TableWireFormat twf = wireMap.get(TableWireFormat.buildNestedTag(parentTag));
            this.nested = true;
            return new InfinispanDocument(messageName, twf.getNestedWireMap(), null);
        }
    }

    public Column getPrimaryKey() {
        Column pkColumn = null;
        if (getTopLevelTable().getPrimaryKey() != null) {
            pkColumn = getTopLevelTable().getPrimaryKey().getColumns().get(0);
        }
        return pkColumn;
    }

    private Object resolveExpressionValue(Expression expr) {
        Object value = null;
        if (expr instanceof Literal) {
            value = ((Literal)expr).getValue();
        }
        else if (expr instanceof org.teiid.language.Array) {
            org.teiid.language.Array contents = (org.teiid.language.Array)expr;
            List<Expression> arrayExprs = contents.getExpressions();
            List<Object> values = new ArrayList<Object>();
            for (Expression exp:arrayExprs) {
                if (exp instanceof Literal) {
                    values.add(((Literal)exp).getValue());
                }
                else {
                    this.exceptions.add(new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25003)));
                }
            }
            value = values;
        }
        else {
            this.exceptions.add(new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25003)));
        }
        return value;
    }


    @Override
    public void visit(Update obj) {
        this.operationType = OperationType.UPDATE;
        append(obj.getTable());
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE).append(SQLConstants.Reserved.WHERE).append(Tokens.SPACE);
            append(obj.getWhere());
        }

        // table that update issued for
        Table table = obj.getTable().getMetadataObject();
        if (!table.equals(getTopLevelTable())) {
            this.nested = true;
        }

        // read the properties
        try {
            int elementCount = obj.getChanges().size();
            for (int i = 0; i < elementCount; i++) {
                Column column = obj.getChanges().get(i).getSymbol().getMetadataObject();
                Expression expr = obj.getChanges().get(i).getValue();
                Object value = resolveExpressionValue(expr);
                String attrName = MarshallerBuilder.getDocumentAttributeName(column, this.nested, this.metadata);
                this.updatePayload.put(attrName, value);
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }

    @Override
    public void visit(Delete obj) {
        this.operationType = OperationType.DELETE;
        append(obj.getTable());

        // table that update issued for
        Table table = obj.getTable().getMetadataObject();
        if (!table.equals(getTopLevelTable())) {
            this.nested = true;
        }

        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE).append(SQLConstants.Reserved.WHERE).append(Tokens.SPACE);
            append(obj.getWhere());
        }
    }

    public List<String> getProjectedColumnNames() {
        ArrayList<String> names = new ArrayList<>();
        for (Expression expr: this.projectedExpressions) {
            if (expr instanceof ColumnReference) {
                names.add(((ColumnReference)expr).getMetadataObject().getName());
            } else if (expr instanceof Function) {
                names.add(((Function)expr).getName());
            }
        }
        return names;
    }

    @Override
    public String getQuery(boolean selectAllColumns) {
        StringBuilder sb = new StringBuilder();
        addSelectedColumns(selectAllColumns, sb);
        sb.append(Tokens.SPACE).append(SQLConstants.Reserved.FROM);
        sb.append(Tokens.SPACE).append(super.toString());
        return  sb.toString();
    }
}
