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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.MessageMarshaller.ProtoStreamReader;
import org.infinispan.protostream.MessageMarshaller.ProtoStreamWriter;
import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.impl.BaseMarshallerDelegate;
import org.infinispan.protostream.impl.ByteArrayOutputStreamEx;
import org.infinispan.protostream.impl.RawProtoStreamWriterImpl;
import org.teiid.infinispan.api.InfinispanDocument;
import org.teiid.infinispan.api.ProtobufDataManager;
import org.teiid.infinispan.api.TableWireFormat;
import org.teiid.infinispan.api.TeiidMarsheller;
import org.teiid.translator.document.Document;

public class TeiidTableMarsheller implements TeiidMarsheller.Marsheller {
    private String documentName;
    private MarshallerDelegate delegate;
    private TreeMap<Integer, TableWireFormat> wireMap = new TreeMap<>();

    public TeiidTableMarsheller(String docName, TreeMap<Integer, TableWireFormat> wireMap) {
        this.documentName= docName;
        this.wireMap = wireMap;
    }

    @Override
    public String getTypeName() {
        return this.documentName;
    }

    @Override
    public BaseMarshallerDelegate<Document> getDelegate() {
        if (delegate == null) {
            delegate = new MarshallerDelegate();
        }
        return delegate;
    }

    // Read from ISPN Types >> Teiid Types
    @Override
    public Object read(RawProtoStreamReader in)  throws IOException {
        InfinispanDocument row = new InfinispanDocument(this.documentName, this.wireMap, null);
        readDocument(in, row, this.wireMap);
        return row;
    }

    // Write from Teiid Types >> ISPN Types
    @Override
    public void write(Object obj, RawProtoStreamWriter out) throws IOException {
        InfinispanDocument document = (InfinispanDocument)obj;
        TreeMap<Integer, TableWireFormat> wireMap = document.getWireMap();
        for (Entry<Integer, TableWireFormat> entry : wireMap.entrySet()) {
            TableWireFormat twf = entry.getValue();

            if (twf == null) {
                throw new IOException("Error in wireformat");
            }

            int tag = twf.getWriteTag();

            if (twf.isNested()) {
                List<? extends Document> children = document.getChildDocuments(twf.getAttributeName());
                if (children != null) {
                    for (Document d : children) {
                        ByteArrayOutputStreamEx baos = new ByteArrayOutputStreamEx();
                        RawProtoStreamWriter rpsw = RawProtoStreamWriterImpl.newInstance(baos);
                        write(d, rpsw);
                        rpsw.flush();
                        baos.flush();
                        // here readtag because this is inner object, even other one uses write tag but calculated
                        // based on the write operation used.
                        out.writeBytes(tag, baos.getByteBuffer());
                    }
                }
                continue;
            }

            Object value = document.getProperties().get(twf.getAttributeName());
            if (value == null) {
                continue;
            }

            ArrayList<Object> values = null;
            boolean array = twf.isArrayType();
            if (array) {
                values = (ArrayList<Object>)value;
            }

            switch (twf.getProtobufType()) {
            case DOUBLE:
                if (array) {
                    for (Object o:values) {
                        out.writeDouble(tag, ProtobufDataManager.convertToInfinispan(Double.class, o));
                    }
                } else {
                    out.writeDouble(tag, ProtobufDataManager.convertToInfinispan(Double.class, value));
                }
                break;
            case FLOAT:
                if (array) {
                    for (Object o:values) {
                        out.writeFloat(tag, ProtobufDataManager.convertToInfinispan(Float.class, o));
                    }
                } else {
                    out.writeFloat(tag, ProtobufDataManager.convertToInfinispan(Float.class, value));
                }
                break;
            case BOOL:
                if (array) {
                    for (Object o:values) {
                        out.writeBool(tag, ProtobufDataManager.convertToInfinispan(Boolean.class, o));
                    }
                } else {
                    out.writeBool(tag, ProtobufDataManager.convertToInfinispan(Boolean.class, value));
                }
                break;
            case STRING:
                if (array) {
                    for (Object o:values) {
                        out.writeString(tag, ProtobufDataManager.convertToInfinispan(String.class, o));
                    }
                } else {
                    out.writeString(tag, ProtobufDataManager.convertToInfinispan(String.class, value));
                }
                break;
            case BYTES:
                if (array) {
                    for (Object o:values) {
                        out.writeBytes(tag, ProtobufDataManager.convertToInfinispan(byte[].class, o));
                    }
                } else {
                    out.writeBytes(tag, ProtobufDataManager.convertToInfinispan(byte[].class, value));
                }
                break;
            case INT32:
                if (array) {
                    for (Object o:values) {
                        out.writeInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case SFIXED32:
                if (array) {
                    for (Object o:values) {
                        out.writeSFixed32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeSFixed32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case FIXED32:
                if (array) {
                    for (Object o:values) {
                        out.writeFixed32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeFixed32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case UINT32:
                if (array) {
                    for (Object o:values) {
                        out.writeUInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeUInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case SINT32:
                if (array) {
                    for (Object o:values) {
                        out.writeSInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeSInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case INT64:
                if (array) {
                    for (Object o:values) {
                        out.writeInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            case UINT64:
                if (array) {
                    for (Object o:values) {
                        out.writeUInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeUInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            case FIXED64:
                if (array) {
                    for (Object o:values) {
                        out.writeFixed64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeFixed64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            case SFIXED64:
                if (array) {
                    for (Object o:values) {
                        out.writeSFixed64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeSFixed64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            case SINT64:
                if (array) {
                    for (Object o:values) {
                        out.writeSInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeSInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            default:
                throw new IOException("Unexpected field type : " + twf.getProtobufType());
            }
        }
    }

    static void readDocument(RawProtoStreamReader in, InfinispanDocument document,
            TreeMap<Integer, TableWireFormat> columnMap) throws IOException {

        while (true) {
            int tag = in.readTag();
            if (tag == 0) {
                break;
            }
            TableWireFormat twf = columnMap.get(tag);
            if (twf == null) {
                throw new IOException("Error in wireformat");
            }

            if (twf.isNested()) {
                InfinispanDocument child = new InfinispanDocument(twf.getAttributeName(), twf.getNestedWireMap(), document);
                int length = in.readRawVarint32();
                int oldLimit = in.pushLimit(length);
                readDocument(in, child, twf.getNestedWireMap());
                document.addChildDocument(twf.getAttributeName(), child);
                in.checkLastTagWas(0);
                in.popLimit(oldLimit);
                continue;
            }

            switch (twf.getProtobufType()) {
            case DOUBLE:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readDouble()));
                break;
            case FLOAT:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readFloat()));
                break;
            case BOOL:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(),in.readBool()));
                break;
            case STRING:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readString()));
                break;
            case BYTES:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readByteArray()));
                break;
            case INT32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readInt32()));
                break;
            case SFIXED32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readSFixed32()));
                break;
            case FIXED32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readFixed32()));
                break;
            case UINT32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readUInt32()));
                break;
            case SINT32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readSInt32()));
                break;
            case INT64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readInt64()));
                break;
            case UINT64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readUInt64()));
                break;
            case FIXED64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readFixed64()));
                break;
            case SFIXED64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readSFixed64()));
                break;
            case SINT64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readSInt64()));
                break;
            default:
                throw new IOException("Unexpected field type : " + twf.getProtobufType());
            }
        }
    }



    class MarshallerDelegate implements BaseMarshallerDelegate<Document> {
        @Override
        public BaseMarshaller<Document> getMarshaller() {
            return new BaseMarshaller<Document>() {
                @Override
                public Class getJavaClass() {
                    return Document.class;
                }

                @Override
                public String getTypeName() {
                    return TeiidTableMarsheller.this.getTypeName();
                }
            };
        }

        @Override
        public void marshall(FieldDescriptor fieldDescriptor, Document value, ProtoStreamWriter writer,
                RawProtoStreamWriter out) throws IOException {
            write(value, out);
        }

        @Override
        public Document unmarshall(FieldDescriptor fieldDescriptor, ProtoStreamReader reader,
                RawProtoStreamReader in) throws IOException {
            return (Document)read(in);
        }
    }
}
