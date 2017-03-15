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

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.hotrod.InfinispanQueryExecution.Paginate;

public class InfinispanDirectExecution implements ProcedureExecution {
    private List<Argument> arguments;
    private Command command;
    private ExecutionContext executionContext;
    private RuntimeMetadata metadata;
    private InfinispanConnection connection;
    private Paginate results;

    public InfinispanDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext,
            RuntimeMetadata metadata, InfinispanConnection connection) {
        this.arguments = arguments;
        this.command = command;
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = connection;
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        return results.getNextRow();
    }

    @Override
    public void execute() throws TranslatorException {
        String queryStr = (String)this.arguments.get(0).getArgumentValue().getValue();
        String cacheName = (String)this.arguments.get(1).getArgumentValue().getValue();

        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "SourceQuery:", queryStr);

        // if the message in defined in different cache than the default, switch it out now.
        RemoteCache<Object, Object> cache =  getCache(cacheName, connection);
        results = new Paginate(cache, queryStr, this.executionContext.getBatchSize(), null, null);
    }

    static RemoteCache<Object, Object> getCache(String cacheName, InfinispanConnection connection) throws TranslatorException {
        RemoteCache<Object, Object> cache = (RemoteCache<Object, Object>)connection.getCache();
        if (cacheName != null && !cacheName.equals(connection.getCache().getName())) {
            cache = ((RemoteCacheManager)connection.getCacheFactory()).getCache(cacheName);
        }
        return cache;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }
}
