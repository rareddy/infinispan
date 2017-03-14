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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.TeiidMarshallerContext;
import org.teiid.infinispan.api.TeiidMarsheller;
import org.teiid.infinispan.api.TeiidTableMarsheller;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class InfinispanQueryExecution implements ResultSetExecution {

    private QueryExpression command;
    private InfinispanConnection connection;
    private RuntimeMetadata metadata;
    private ExecutionContext executionContext;
    private Paginate results;

    public InfinispanQueryExecution(InfinispanExecutionFactory translator,
            QueryExpression command, ExecutionContext executionContext,
            RuntimeMetadata metadata, InfinispanConnection connection) throws TranslatorException {
        this.command = command;
        this.connection = connection;
        this.metadata = metadata;
        this.executionContext = executionContext;
    }

    @Override
    public void execute() throws TranslatorException {
        try {
            IckleConvertionVisitor visitor = new IckleConvertionVisitor(metadata, false);
            visitor.append(this.command);
            Table table = visitor.getTable();
            TeiidMarshallerContext.setMarsheller(getMarsheller(table));
            String queryStr = visitor.getQuery(false);
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "SourceQuery:", queryStr);

            // if the message in defined in different cache than the default, switch it out now.
            RemoteCache<Object, Object> cache =  getCache(table, connection);
            results = new Paginate(cache, queryStr, this.executionContext.getBatchSize(), visitor.getRowLimit(),
                    visitor.getRowOffset());
        } finally {
            TeiidMarshallerContext.setMarsheller(null);
        }
    }


    private TeiidMarsheller.Marsheller getMarsheller(Table table) {
        ArrayList<Integer> tagOrder = new ArrayList<>();
        ArrayList<Class<?>> types = new ArrayList<>();
        for (Column column : table.getColumns()) {
            if (column.isSelectable()) {
                tagOrder.add(ProtobufMetadataProcessor.getTag(column));
                types.add(column.getJavaType());
            }
        }
        return new TeiidTableMarsheller(ProtobufMetadataProcessor.getMessageName(table), tagOrder, types);
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        return results.getNextRow();
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    static RemoteCache<Object, Object> getCache(Table table, InfinispanConnection connection) throws TranslatorException {
        RemoteCache<Object, Object> cache = (RemoteCache<Object, Object>)connection.getCache();
        String cacheName = table.getProperty(ProtobufMetadataProcessor.CACHE, false);
        if (cacheName != null && !cacheName.equals(connection.getCache().getName())) {
            cache = ((RemoteCacheManager)connection.getCacheFactory()).getCache(cacheName);
        }
        return cache;
    }

    class Paginate {
        private Query query;
        private int batchSize;
        private Integer offset;
        private Integer limit;
        private boolean lastBatch = false;
        private Iterator<Object> responseIter;

        public Paginate(RemoteCache<Object, Object> cache, String queryStr, int batchSize, Integer limit,
                Integer offset) {
            this.batchSize = batchSize;
            this.offset = offset == null?0:offset;
            this.limit = limit;

            QueryFactory qf = Search.getQueryFactory(cache);
            this.query = qf.create(queryStr);
        }

        void fetchNextBatch() {
            query.startOffset(offset);

            int nextBatch = this.batchSize;
            if (this.limit != null) {
                if (this.limit > nextBatch) {
                    this.limit = this.limit - nextBatch;
                } else {
                    nextBatch = this.limit;
                    this.limit = 0;
                    this.lastBatch = true;
                }
            }
            query.maxResults(nextBatch);
            List<Object> values = query.list();

            if (query.getResultSize() < nextBatch) {
                this.lastBatch = true;
            }

            this.responseIter = values.iterator();
            offset = offset + nextBatch;
            query.startOffset(offset);
            values = query.list();
        }

        public List<Object> getNextRow(){
            if (responseIter == null) {
                fetchNextBatch();
            }

            if (responseIter != null && responseIter.hasNext()){
                Object[] row = (Object[])this.responseIter.next();
                return Arrays.asList(row);
            } else {
                if (!lastBatch) {
                    fetchNextBatch();
                    Object[] row = (Object[])this.responseIter.next();
                    return Arrays.asList(row);
                }
            }
            return null;
        }
    }
}
