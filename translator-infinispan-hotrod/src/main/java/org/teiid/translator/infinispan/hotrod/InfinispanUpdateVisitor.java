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
import java.util.Map;
import java.util.TreeMap;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.language.ColumnReference;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Function;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;

public class InfinispanUpdateVisitor extends IckleConvertionVisitor {
    protected enum OperationType {INSERT, UPDATE, DELETE, UPSERT};
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private OperationType operationType;
    private Map<String, Object> payload = new TreeMap<>();
    private Object identity;

    public InfinispanUpdateVisitor(RuntimeMetadata metadata) {
        super(metadata, true);
    }

    public Object getIdentity() {
        return identity;
    }

    public OperationType getOperationType() {
        return this.operationType;
    }


    public Map<String, Object> getPayload() {
        return payload;
    }

    @Override
    public void visit(Insert obj) {
        this.operationType = OperationType.INSERT;
        if (obj.isUpsert()) {
            this.operationType = OperationType.UPSERT;
        }
        visitNode(obj.getTable());

        Column pkColumn = getPrimaryKey();

        // read the properties
        int elementCount = obj.getColumns().size();
        for (int i = 0; i < elementCount; i++) {
            Column column = obj.getColumns().get(i).getMetadataObject();
            List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();
            Expression expr = values.get(i);
            Object value = resolveExpressionValue(expr);
            this.payload.put(column.getName(), value);

            if (pkColumn != null && column.equals(pkColumn)) {
                this.identity = value;
            }
        }

        // add default values in
        try {
            for (Column column : getTable().getColumns()) {
                if (column.getDefaultValue() != null && !this.payload.containsKey(column.getName())) {
                    this.payload.put(column.getName(), DataTypeManager.getTransform(String.class, column.getJavaType())
                            .transform(column.getDefaultValue(), column.getJavaType()));
                }
            }
        } catch (NumberFormatException e) {
            this.exceptions.add(new TranslatorException(e));
        } catch (TransformationException e) {
            this.exceptions.add(new TranslatorException(e));
        }

        if (this.identity == null) {
            this.exceptions.add(new TranslatorException(
                    InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25004, getTable().getName())));
        }
    }

    public Column getPrimaryKey() {
        Column pkColumn = null;
        if (getTable().getPrimaryKey() != null) {
            pkColumn = getTable().getPrimaryKey().getColumns().get(0);
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

        // read the properties
        int elementCount = obj.getChanges().size();
        for (int i = 0; i < elementCount; i++) {
            Column column = obj.getChanges().get(i).getSymbol().getMetadataObject();
            Expression expr = obj.getChanges().get(i).getValue();
            Object value = resolveExpressionValue(expr);
            this.payload.put(column.getName(), value);
        }
    }

    @Override
    public void visit(Delete obj) {
        this.operationType = OperationType.DELETE;
        append(obj.getTable());
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
