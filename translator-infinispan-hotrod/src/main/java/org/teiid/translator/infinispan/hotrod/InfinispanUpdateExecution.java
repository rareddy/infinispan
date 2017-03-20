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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.TeiidMarshallerContext;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.infinispan.hotrod.InfinispanUpdateVisitor.OperationType;

public class InfinispanUpdateExecution implements UpdateExecution {
    private int updateCount = 0;
    private Command command;
    private InfinispanConnection connection;
    private ExecutionContext executionContext;
    private RuntimeMetadata metadata;

    public InfinispanUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata,
            InfinispanConnection connection) throws TranslatorException {
        this.command = command;
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = connection;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute() throws TranslatorException {

        final InfinispanUpdateVisitor visitor = new InfinispanUpdateVisitor(this.metadata);
        visitor.append(this.command);

        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }

        try {
            Table table = visitor.getTable();
            TeiidMarshallerContext.setMarsheller(MarshallerBuilder.getMarshaller(table, this.metadata));
            final String PK = table.getPrimaryKey().getColumns().get(0).getName();

            // if the message in defined in different cache than the default, switch it out now.
            final RemoteCache<Object,Object> cache = InfinispanQueryExecution.getCache(table, connection);


            if (visitor.getOperationType() == OperationType.DELETE) {
                paginateResults(cache, visitor.getQuery(false), new Task() {
                    @Override
                    public void run(List<Object> rows) {
                        for (Object obj:rows) {
                            if (cache.remove(((Object[])obj)[0]) != null) {
                                updateCount++;
                            }
                        }
                    }
                }, this.executionContext.getBatchSize());
            } else if (visitor.getOperationType() == OperationType.UPDATE) {
                paginateResults(cache, visitor.getQuery(true), new Task() {
                    @Override
                    public void run(List<Object> rows) {
                        for (Object obj:rows) {
                            Map<String, Object> updated = mergeUpdatePayload(visitor.getProjectedColumnNames(),
                                    (Object[]) obj, visitor.getPayload());
                            cache.replace(updated.get(PK), updated);
                            updateCount++;
                        }
                    }
                }, this.executionContext.getBatchSize());
            } else if (visitor.getOperationType() == OperationType.INSERT) {
                // this is always single row; putIfAbsent is not working correctly.
                Map<String, Object> previous = (Map<String, Object>) cache.get(visitor.getIdentity());
                if (previous == null) {
                    previous = (Map<String, Object>) cache.put(visitor.getIdentity(), visitor.getPayload());
                    this.updateCount++;
                } else {
                    throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25005,
                            table.getName(), visitor.getIdentity()));
                }
            } else if (visitor.getOperationType() == OperationType.UPSERT) {
                // this is always single row; putIfAbsent is not working correctly.
                Map<String, Object> previous = (Map<String, Object>) cache.get(visitor.getIdentity());
                if (previous == null) {
                    previous = (Map<String, Object>) cache.put(visitor.getIdentity(), visitor.getPayload());
                    this.updateCount++;
                } else {
                    cache.put(visitor.getIdentity(), mergePayload(previous, visitor.getPayload()));
                    this.updateCount++;
                }
            }
        } finally {
            TeiidMarshallerContext.setMarsheller(null);
        }
    }

    interface Task {
        void run(List<Object> rows);
    }

    static void paginateResults(RemoteCache<Object,Object> cache, String queryStr, Task task, int batchSize) {

        QueryFactory qf = Search.getQueryFactory(cache);
        Query query = qf.create(queryStr);

        int offset = 0;
        query.startOffset(0);
        query.maxResults(batchSize);
        List<Object> values = query.list();
        while (true) {
            task.run(values);
            if (query.getResultSize() < batchSize) {
                break;
            }
            offset = offset + batchSize;
            query.startOffset(offset);
            values = query.list();
        }
    }


    private Object mergePayload(Map<String, Object> original, Map<String, Object> updates) {
        for (Entry<String, Object> entry : updates.entrySet()) {
            original.put(entry.getKey(), entry.getValue());
        }
        return original;
    }

    private Map<String, Object> mergeUpdatePayload(List<String> projected, Object[] row,
            Map<String, Object> updates) {
        Map<String, Object> updatedRow = new TreeMap<>();
        for (int i = 0; i < projected.size(); i++) {
            String column = projected.get(i);
            Object updatedValue = updates.get(column);
            updatedRow.put(column, updatedValue != null ? updatedValue : row[i]);
        }
        return updatedRow;
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        return new int[] {this.updateCount};
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }
}
