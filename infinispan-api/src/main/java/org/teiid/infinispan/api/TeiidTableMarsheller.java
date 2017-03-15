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
import java.util.Map;
import java.util.TreeMap;

import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.MessageMarshaller.ProtoStreamReader;
import org.infinispan.protostream.MessageMarshaller.ProtoStreamWriter;
import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.impl.BaseMarshallerDelegate;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.document.Document;

public class TeiidTableMarsheller implements TeiidMarsheller.Marsheller {
    private static final String TAG = MetadataFactory.ODATA_URI+"TAG"; //$NON-NLS-1$
    private static final String PARENT_TAG = MetadataFactory.ODATA_URI+"PARENT_TAG"; //$NON-NLS-1$
    private static final String MESSAGE = MetadataFactory.ODATA_URI+"MESSAGE"; //$NON-NLS-1$
    public static final String MESSAGE_NAME = MetadataFactory.ODATA_URI+"MESSAGE_NAME"; //$NON-NLS-1$

    private Table table;
    private MarshallerDelegate delegate;
    private RuntimeMetadata metadata;
    private List<Table> childTables;

    public TeiidTableMarsheller(Table table, List<Table> childTables) {
        this.table = table;
        this.childTables = childTables;
    }

    // Read from ISPN Types >> Teiid Types
    @Override
    public Object read(RawProtoStreamReader in)  throws IOException {
        TreeMap<String, List<Column>> inlinedColumnsByParent = new TreeMap<>();

        Document row = new Document(table.getName(), false, null);
        for (Column column: table.getColumns()) {
            String parentTag = column.getProperty(PARENT_TAG, false);
            if ( parentTag == null) {
                int tag = Integer.parseInt(column.getProperty(TAG, false));
                Class<?> type = column.getJavaType();
                // TODO: test reading the primitive array value
                row.addProperty(column.getName(), TeiidMarsheller.readAttribute(in, type, tag));
            } else {
                String msgName = column.getProperty(MESSAGE_NAME, false);
                List<Column> inlinedColumns = inlinedColumnsByParent.get(msgName);
                if (inlinedColumns == null) {
                    inlinedColumns = new ArrayList<>();
                    inlinedColumnsByParent.put(msgName, inlinedColumns);
                }
                inlinedColumns.add(column);
            }
        }

        // read inlined columns
        if (!inlinedColumnsByParent.isEmpty()) {
            for (Map.Entry<String, List<Column>> entry : inlinedColumnsByParent.entrySet()) {
                //for
            }
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

    @Override
    public BaseMarshallerDelegate<Map<String, Object>> getDelegate() {
        if (delegate == null) {
            delegate = new MarshallerDelegate();
        }
        return delegate;
    }

    class MarshallerDelegate implements BaseMarshallerDelegate<Map<String, Object>> {
        @Override
        public BaseMarshaller<Map<String, Object>> getMarshaller() {
            return new BaseMarshaller<Map<String, Object>>() {
                @Override
                public Class getJavaClass() {
                    return Map.class;
                }

                @Override
                public String getTypeName() {
                    return getTypeName();
                }
            };
        }

        @Override
        public void marshall(FieldDescriptor fieldDescriptor, Map<String, Object> value, ProtoStreamWriter writer,
                RawProtoStreamWriter out) throws IOException {
            write(value, out);
        }

        @Override
        public Map<String, Object> unmarshall(FieldDescriptor fieldDescriptor, ProtoStreamReader reader,
                RawProtoStreamReader in) throws IOException {
            return (Map<String, Object>)read(in);
        }
    }
}
