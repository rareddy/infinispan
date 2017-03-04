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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.TeiidMarshallerContext;
import org.teiid.infinispan.api.TeiidQueryMarsheller;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class InfinispanQueryExecution implements ResultSetExecution {
    
    private Class<?>[] expectedColumnTypes;
    private QueryExpression command;
    private InfinispanConnection connection;
    private Iterator<Object[]> responseIter;
    
    public InfinispanQueryExecution(InfinispanExecutionFactory translator,
            QueryExpression command, ExecutionContext executionContext,
            RuntimeMetadata metadata, InfinispanConnection connection) throws TranslatorException {
        this.command = command; 
        this.connection = connection;
        this.expectedColumnTypes = command.getColumnTypes();
    }

    @Override
    public void execute() throws TranslatorException {
        IckleConvertionVisitor visitor = new IckleConvertionVisitor();
        visitor.append(this.command);
        String queryStr = visitor.toString();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "SourceQuery:", queryStr);
        
        RemoteCache<?,?> cache = null;
        String cacheName = visitor.getCacheName();
        if (cacheName != null) {
            cache = (RemoteCache<?,?>)connection.getCacheFactory().getCache(cacheName);
        } else {
            cache = (RemoteCache<?,?>)connection.getCacheFactory().getCache();
        }
        
        QueryFactory qf = Search.getQueryFactory(cache);
        Query query = qf.create(queryStr);
        try {
            TeiidMarshallerContext.setMarsheller(
                    new TeiidQueryMarsheller(visitor.getMessageType(),
                            visitor.getTagOrder(), expectedColumnTypes));
            List<Object[]> response = query.list();
            if (response == null) {
                response = Collections.emptyList();
            }
            this.responseIter = response.iterator();
        } finally {
            TeiidMarshallerContext.setMarsheller(null);
        }
    }
    
    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (!this.responseIter.hasNext()) {
            return null;
        }
        Object[] row = this.responseIter.next();
        return Arrays.asList(row);
    }    
    
    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }    
}
