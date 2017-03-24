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

package org.teiid.resource.adapter.infinispan.hotrod;


import java.util.HashMap;

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;


public class InfinispanConnectionImpl extends BasicConnection implements InfinispanConnection {
    private RemoteCacheManager manager;
    private String cacheName;
    private HashMap<String, Boolean> registered = new HashMap<>();
    private BasicCache<?, ?> defaultCache;

    public InfinispanConnectionImpl(RemoteCacheManager manager, String cacheName) throws ResourceException {
        this.manager = manager;
        this.cacheName = cacheName;
        try {
            this.defaultCache = this.manager.getCache(this.cacheName);
        } catch (Throwable t) {
            throw new ResourceException(t);
        }
    }

    @Override
    public void registerProtobufFile(ProtobufResource protobuf) throws TranslatorException {
        try {
            if (protobuf != null && registered.get(protobuf.getIdentifier()) == null) {
                RemoteCache<String, String> metadataCache = manager
                        .getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
                if (metadataCache.get(protobuf.getIdentifier()) == null) {
                    metadataCache.put(protobuf.getIdentifier(), protobuf.getContents());
                    String errors = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
                    if (errors != null) {
                       throw new TranslatorException(InfinispanManagedConnectionFactory.UTIL.getString("proto_error", errors));
                    }
                    registered.put(protobuf.getIdentifier(), Boolean.TRUE);
                }

            }
        } catch(Throwable t) {
            throw new TranslatorException(t);
        }
    }

    @Override
    public BasicCacheContainer getCacheFactory() throws TranslatorException {
        return this.manager;
    }

    @Override
    public void close() throws ResourceException {
        // do not want to close on per cache basis
        // TODO: what needs to be done here?
    }

    @Override
    public BasicCache getCache() throws TranslatorException {
        return defaultCache;
    }
}