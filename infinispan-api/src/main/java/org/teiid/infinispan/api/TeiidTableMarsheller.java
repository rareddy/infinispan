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

public class TeiidTableMarsheller implements TeiidMarsheller.Marsheller {
    private String messageType;
    private List<Integer> tagOrder;
    private List<Class<?>> expectedColumnTypes;

    public TeiidTableMarsheller(String messageType, List<Integer> tagOrder, List<Class<?>> expectedColumnTypes) {
        this.messageType = messageType;
        this.tagOrder = tagOrder;
        this.expectedColumnTypes = expectedColumnTypes;
    }

    // Write from Teiid Types >> ISPN Types
    @Override
    public void write (Object obj, RawProtoStreamWriter out) throws IOException {
        List<Object> row = (List<Object>) obj;
        for (int i = 0; i < tagOrder.size(); i++) {
            int tag = tagOrder.get(i);
            Object value = row.get(i);
            TeiidMarsheller.writeAttribute(out, tag, value);
        }
    }

    // Read from ISPN Types >> Teiid Types
    @Override
    public Object read(String name, RawProtoStreamReader in)  throws IOException {
        List<Object> row = new ArrayList<>();
        for (int i = 0; i < tagOrder.size(); i++) {
            Class<?> type = expectedColumnTypes.get(i);
            int tag = tagOrder.get(i);
            row.add(TeiidMarsheller.readAttribute(in, type, tag));
        }
        return row;
    }

    @Override
    public String getTypeName() {
        return this.messageType;
    }
}
