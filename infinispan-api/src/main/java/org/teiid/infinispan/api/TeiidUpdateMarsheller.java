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
import java.util.TreeMap;

import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;

public class TeiidUpdateMarsheller implements TeiidMarsheller.Marsheller {
    private static final String TAG = MetadataFactory.ODATA_URI+"TAG"; //$NON-NLS-1$
    private static final String MESSAGE = MetadataFactory.ODATA_URI+"MESSAGE"; //$NON-NLS-1$
    private Table table;

    public TeiidUpdateMarsheller(Table table) {
        this.table = table;
    }

    // Read from ISPN Types >> Teiid Types
    @Override
    public Object read(String name, RawProtoStreamReader in)  throws IOException {
        Map<String, Object> row = new TreeMap<>();
        for (Column column: table.getColumns()) {
            int tag = Integer.parseInt(column.getProperty(TAG, false));
            Class<?> type = column.getJavaType();
            row.put(column.getName(), TeiidMarsheller.readAttribute(in, type, tag));
        }
        return row;
    }

    // Write from Teiid Types >> ISPN Types
    @Override
    public void write(Object obj, RawProtoStreamWriter out) throws IOException {
        Map<String, Object> row = (Map<String, Object>)obj;
        for (Column column: table.getColumns()) {
            int tag = Integer.parseInt(column.getProperty(TAG, false));
            Class<?> type = column.getJavaType();
            Object value = row.get(column.getName());
            TeiidMarsheller.writeAttribute(out, tag, value);
        }
    }

    @Override
    public String getTypeName() {
        return table.getProperty(MESSAGE, false);
    }
}
