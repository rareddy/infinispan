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

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.teiid.core.BundleUtil;
import org.teiid.infinispan.api.TeiidMarsheller;
import org.teiid.infinispan.api.TeiidSerializationContext;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;


public class InfinispanManagedConnectionFactory extends BasicManagedConnectionFactory {
    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(InfinispanManagedConnectionFactory.class);
    private static final long serialVersionUID = -4791974803005018658L;

    private String remoteServerList;
    private String cacheName;

    public String getRemoteServerList() {
        return remoteServerList;
    }

    public void setRemoteServerList(String remoteServerList) {
        this.remoteServerList = remoteServerList;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    @Override
    public BasicConnectionFactory<InfinispanConnectionImpl> createConnectionFactory()
            throws ResourceException {
        return new InfinispanConnectionFactory();
    }

    class InfinispanConnectionFactory extends BasicConnectionFactory<InfinispanConnectionImpl> {
        private static final long serialVersionUID = 1064143496037686580L;
        private RemoteCacheManager cacheManager;

        public InfinispanConnectionFactory() throws ResourceException {
            try {
                SerializationContext baseCtx = ProtobufUtil.newSerializationContext(Configuration.builder().build());
                TeiidSerializationContext ctx = new TeiidSerializationContext(baseCtx);
                TeiidMarsheller marsheller = new TeiidMarsheller(ctx);
                ConfigurationBuilder builder = new ConfigurationBuilder();
                builder.addServers(remoteServerList);
                builder.marshaller(marsheller);

                // note this object is expensive, so there needs to only one
                // instance for the JVM, in this case one per RA instance.
                this.cacheManager = new RemoteCacheManager(builder.build());

                // register default marshellers
                /*
                SerializationContext ctx = ProtoStreamMarshaller.getSerializationContext(this.cacheManager);
                FileDescriptorSource fds = new FileDescriptorSource();
                ctx.registerProtoFiles(fds);
                */
                this.cacheManager.start();
            } catch (Throwable e) {
                throw new ResourceException(e);
            }
        }

        @Override
        public InfinispanConnectionImpl getConnection() throws ResourceException {
            return new InfinispanConnectionImpl(this.cacheManager, cacheName);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((remoteServerList == null) ? 0 : remoteServerList.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InfinispanManagedConnectionFactory other = (InfinispanManagedConnectionFactory) obj;
        if (remoteServerList == null) {
            if (other.remoteServerList != null)
                return false;
        } else if (!remoteServerList.equals(other.remoteServerList))
            return false;
        return true;
    }
}