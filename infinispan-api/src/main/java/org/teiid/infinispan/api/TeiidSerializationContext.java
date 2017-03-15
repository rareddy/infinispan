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
package org.teiid.infinispan.api;

import java.io.IOException;
import java.util.Map;

import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.EnumDescriptor;
import org.infinispan.protostream.descriptors.FileDescriptor;
import org.infinispan.protostream.descriptors.GenericDescriptor;
import org.infinispan.protostream.impl.BaseMarshallerDelegate;

public class TeiidSerializationContext implements SerializationContext {

    private SerializationContext delegate;

    public TeiidSerializationContext(SerializationContext delegate) {
        this.delegate = delegate;
    }

    @Override
    public Configuration getConfiguration() {
        return delegate.getConfiguration();
    }

    @Override
    public Map<String, FileDescriptor> getFileDescriptors() {
        return delegate.getFileDescriptors();
    }

    @Override
    public void registerProtoFiles(FileDescriptorSource source) throws IOException, DescriptorParserException {
        delegate.registerProtoFiles(source);
    }

    @Override
    public Descriptor getMessageDescriptor(String fullName) {
        return delegate.getMessageDescriptor(fullName);
    }

    @Override
    public EnumDescriptor getEnumDescriptor(String fullName) {
        return delegate.getEnumDescriptor(fullName);
    }

    @Override
    public boolean canMarshall(Class<?> clazz) {
        return delegate.canMarshall(clazz);
    }

    @Override
    public void unregisterProtoFile(String fileName) {
        delegate.unregisterProtoFile(fileName);
    }

    @Override
    public <T> void registerMarshaller(BaseMarshaller<T> marshaller) {
        delegate.registerMarshaller(marshaller);
    }

    @Override
    public boolean canMarshall(String fullName) {
        if (TeiidMarshallerContext.getMarsheller().getTypeName().equals(fullName)) {
            return true;
        }
        return delegate.canMarshall(fullName);
    }

    @Override
    public <T> BaseMarshaller<T> getMarshaller(String fullName) {
        TeiidMarsheller.Marsheller m = TeiidMarshallerContext.getMarsheller();
        if (m != null && m.getTypeName().equals(fullName)) {
            // TODO: not sure if this is called
            return null;
        }
        return delegate.getMarshaller(fullName);
    }

    @Override
    public <T> BaseMarshaller<T> getMarshaller(Class<T> clazz) {
        return delegate.getMarshaller(clazz);
    }

    @Override
    public String getTypeNameById(Integer typeId) {
        return delegate.getTypeNameById(typeId);
    }

    @Override
    public Integer getTypeIdByName(String fullName) {
        return delegate.getTypeIdByName(fullName);
    }

    @Override
    public GenericDescriptor getDescriptorByTypeId(Integer typeId) {
        return delegate.getDescriptorByTypeId(typeId);
    }

    @Override
    public GenericDescriptor getDescriptorByName(String fullName) {
        return delegate.getDescriptorByName(fullName);
    }

    @Override
    public <T> BaseMarshallerDelegate<T> getMarshallerDelegate(String descriptorFullName) {
        TeiidMarsheller.Marsheller m = TeiidMarshallerContext.getMarsheller();
        if (m != null && descriptorFullName.equals(m.getTypeName())) {
            return m.getDelegate();
        }
        return delegate.getMarshallerDelegate(descriptorFullName);
    }

    @Override
    public <T> BaseMarshallerDelegate<T> getMarshallerDelegate(Class<T> clazz) {
        return delegate.getMarshallerDelegate(clazz);
    }
}
