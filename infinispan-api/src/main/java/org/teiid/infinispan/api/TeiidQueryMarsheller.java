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
import java.util.ArrayList;
import java.util.List;

import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.protostream.impl.WireFormat;

public class TeiidQueryMarsheller {
    private String messageType;
    private List<Integer> tagOrder;
    private Class<?>[] expectedColumnTypes;
    
    public TeiidQueryMarsheller(String messageType, List<Integer> tagOrder, Class<?>[] expectedColumnTypes) {
        this.messageType = messageType;
        this.tagOrder = tagOrder;
        this.expectedColumnTypes = expectedColumnTypes;
    }
    
    public String write (Object obj, RawProtoStreamWriter out) throws IOException {
        List<Object> row = (List) obj;
        for (int i = 0; i < tagOrder.size(); i++) {
            Class<?> type = expectedColumnTypes[i];
            if (type.isAssignableFrom(String.class)) {
                out.writeString(tagOrder.get(i), (String)row.get(i));        
            } else if (type.isAssignableFrom(int.class)) {
                out.writeInt32(tagOrder.get(i), (Integer)row.get(i));        
            } else if (type.isAssignableFrom(long.class)) {
                out.writeInt64(tagOrder.get(i), (Long)row.get(i));        
            } else if (type.isAssignableFrom(boolean.class)) {
                out.writeBool(tagOrder.get(i), (Boolean)row.get(i));        
            } else if (type.isAssignableFrom(float.class)) {
                out.writeFloat(tagOrder.get(i), (Float)row.get(i));        
            } else if (type.isAssignableFrom(double.class)) {
                out.writeDouble(tagOrder.get(i), (Double)row.get(i));        
            } else if (type.isAssignableFrom(byte[].class)) {
                out.writeBytes(tagOrder.get(i), (byte[])row.get(i));        
            } else {
                throw new IOException("unknown type");
            }
        }
        return messageType;
    }
    
    public Object read(String name, RawProtoStreamReader in)  throws IOException {
        List<Object> row = new ArrayList<>();
        for (int i = 0; i < tagOrder.size(); i++) {
            Class<?> type = expectedColumnTypes[i];
            Type protoType = Type.STRING;
            if (type.isAssignableFrom(String.class)) {
                protoType = Type.STRING;        
            } else if (type.isAssignableFrom(int.class)) {
                protoType = Type.INT32;        
            } else if (type.isAssignableFrom(long.class)) {
                protoType = Type.INT64;        
            } else if (type.isAssignableFrom(boolean.class)) {
                protoType = Type.BOOL;        
            } else if (type.isAssignableFrom(float.class)) {
                protoType = Type.FLOAT;        
            } else if (type.isAssignableFrom(double.class)) {
                protoType = Type.DOUBLE;        
            } else if (type.isAssignableFrom(byte[].class)) {
                protoType = Type.BYTES;        
            } else {
                throw new IOException("unknown type");
            }
            row.add(TeiidMarsheller.getValue(in, protoType, WireFormat.makeTag(tagOrder.get(i),  protoType.getWireType())));    
        }
        return row;
    }
}
