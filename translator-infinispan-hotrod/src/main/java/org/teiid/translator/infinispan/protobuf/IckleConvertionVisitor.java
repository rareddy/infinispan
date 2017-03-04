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
package org.teiid.translator.infinispan.protobuf;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.NamedTable;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;

public class IckleConvertionVisitor extends SQLStringVisitor {
    private String cacheName;
    private String messageType;
    private List<Integer> tagOrder = new ArrayList<>();
    
    public List<Integer> getTagOrder() {
        return tagOrder;
    }

    public String getCacheName() {
        return this.cacheName;
    }

    public String getMessageType() {
        return messageType;
    }
    
    @Override
    public void visit(NamedTable obj) {
        Table t = obj.getMetadataObject();
        this.cacheName = t.getProperty(ProtobufMetadataProcessor.CACHE, false);
        this.messageType = t.getProperty(ProtobufMetadataProcessor.MESSAGE, false);
        super.visit(obj);
    }    
    
    public void visit(DerivedColumn obj) {
        if (obj.getExpression() instanceof ColumnReference) {
            Column column= ((ColumnReference)obj.getExpression()).getMetadataObject();
            tagOrder.add(Integer.parseInt(column.getProperty(ProtobufMetadataProcessor.TAG, false)));
        }
        super.visit(obj);
    }
    
    /*
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected QueryExpression command;
    protected InfinispanExecutionFactory executionFactory;
    protected RuntimeMetadata metadata;
    protected ArrayList<Column> projectedColumns = new ArrayList<Column>();
    private StringBuilder buffer = new StringBuilder();

    public List<Column> getProjectedColumns(){
        return this.projectedColumns;
    }
    
    public String getQuery() {
        return this.ickleQuery.toString();
    }

    List<String> getColumnNames(List<Column> columns) {
        ArrayList<String> names = new ArrayList<String>();
        for (Column c : columns) {
            names.add(c.getName());
        }
        return names;
    }    
    
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
    
    @Override
    public void visit(Limit obj) {
        if (obj.getRowOffset() != 0) {
            this.odataQuery.setSkip(new Integer(obj.getRowOffset()));
        }
        if (obj.getRowLimit() != 0) {
            this.odataQuery.setTop(new Integer(obj.getRowLimit()));
        }
    }

    @Override
    public void visit(OrderBy obj) {
         append(obj.getSortSpecifications());
    }

    @Override
    public void visit(SortSpecification obj) {
        if (this.orderBy.length() > 0) {
            this.orderBy.append(Tokens.COMMA);
        }
        ColumnReference column = (ColumnReference)obj.getExpression();
        try {
            Column c = normalizePseudoColumn(column.getMetadataObject());
            this.orderBy.append(c.getName());
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
        // default is ascending
        if (obj.getOrdering() == Ordering.DESC) {
            this.orderBy.append(Tokens.SPACE).append(DESC.toLowerCase());
        }
    }

    @Override
    public void visit(Select obj) {
        buffer.append("SELECT ");        
        visitNodes(obj.getDerivedColumns());
        
        buffer.append(" FROM ");
        visitNodes(obj.getFrom());
        
        visitNode(obj.getWhere());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
    }

    @Override
    public void visit(DerivedColumn obj) {
        if (obj.getExpression() instanceof ColumnReference) {
            Column column = ((ColumnReference)obj.getExpression()).getMetadataObject();
            if (!column.isSelectable()) {
                this.exceptions.add(new TranslatorException(InfinispanPlugin.Util
                        .gs(InfinispanPlugin.Event.TEIID25001, column.getName())));
            }
            this.projectedColumns.add(column);
        }
        else if (obj.getExpression() instanceof AggregateFunction) {
            AggregateFunction func = (AggregateFunction)obj.getExpression();
            // TODO: sum, avg etc.
        }
        else if (obj.getExpression() instanceof Function) {
            // TODO: should not be any
        }
        else {
            this.exceptions.add(new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25002, obj)));
        }
    }

    public void append(LanguageObject obj) {
        visitNode(obj);
    }

    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                buffer.append(Tokens.COMMA)
                      .append(Tokens.SPACE);
                append(items.get(i));
            }
        }
    }

    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            append(items[0]);
            for (int i = 1; i < items.length; i++) {
                buffer.append(Tokens.COMMA)
                      .append(Tokens.SPACE);
                append(items[i]);
            }
        }
    }
    */
}
