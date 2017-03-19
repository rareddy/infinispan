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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.protostream.impl.BaseMarshallerDelegate;
import org.infinispan.protostream.impl.ByteArrayOutputStreamEx;
import org.infinispan.protostream.impl.RawProtoStreamReaderImpl;
import org.infinispan.protostream.impl.RawProtoStreamWriterImpl;
import org.infinispan.protostream.impl.WireFormat;
import org.teiid.core.util.ObjectConverterUtil;

public class TeiidMarsheller extends ProtoStreamMarshaller {
    private SerializationContext ctx;

    public interface Marsheller {
        Object read(RawProtoStreamReader in) throws IOException;

        void write(Object obj, RawProtoStreamWriter out) throws IOException;

        String getTypeName();

        <T> BaseMarshallerDelegate<T> getDelegate();
    }

    @Override
    public boolean isMarshallable(Object o) {
        if (super.isMarshallable(o)) {
            return true;
        }
        // this handles list or map types
        if (o instanceof List || o instanceof Map) {
            return true;
        }
        return false;
    }

    public TeiidMarsheller(SerializationContext ctx) {
        this.ctx = ctx;
     }

    @Override
    public SerializationContext getSerializationContext() {
       return this.ctx;
    }

    @Override
    public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(buf, offset, length);
        RawProtoStreamReader reader = RawProtoStreamReaderImpl.newInstance(bais);
        return readMessage(reader);
    }

    public static Object readMessage(RawProtoStreamReader in) throws IOException {
        String descriptorFullName = null;
        Integer typeId = null;
        int enumValue = -1;
        byte[] messageBytes = null;
        Object value = null;
        int readTags = 0;

        int tag;
        while ((tag = in.readTag()) != 0) {
            readTags++;
            switch (tag) {
            case WrappedMessage.WRAPPED_DESCRIPTOR_FULL_NAME << 3 | WireFormat.WIRETYPE_LENGTH_DELIMITED:
                descriptorFullName = in.readString();
                break;
            case WrappedMessage.WRAPPED_DESCRIPTOR_ID << 3 | WireFormat.WIRETYPE_VARINT:
                typeId = in.readInt32();
                break;
            case WrappedMessage.WRAPPED_ENUM << 3 | WireFormat.WIRETYPE_VARINT:
                enumValue = in.readEnum();
                break;
            case WrappedMessage.WRAPPED_MESSAGE << 3 | WireFormat.WIRETYPE_LENGTH_DELIMITED:
                messageBytes = in.readByteArray();
                break;
            case WrappedMessage.WRAPPED_STRING << 3 | WireFormat.WIRETYPE_LENGTH_DELIMITED:
                value = in.readString();
                break;
            case WrappedMessage.WRAPPED_BYTES << 3 | WireFormat.WIRETYPE_LENGTH_DELIMITED:
                value = in.readByteArray();
                break;
            case WrappedMessage.WRAPPED_BOOL << 3 | WireFormat.WIRETYPE_VARINT:
                value = in.readBool();
                break;
            case WrappedMessage.WRAPPED_DOUBLE << 3 | WireFormat.WIRETYPE_FIXED64:
                value = in.readDouble();
                break;
            case WrappedMessage.WRAPPED_FLOAT << 3 | WireFormat.WIRETYPE_FIXED32:
                value = in.readFloat();
                break;
            case WrappedMessage.WRAPPED_FIXED32 << 3 | WireFormat.WIRETYPE_FIXED32:
                value = in.readFixed32();
                break;
            case WrappedMessage.WRAPPED_SFIXED32 << 3 | WireFormat.WIRETYPE_FIXED32:
                value = in.readSFixed32();
                break;
            case WrappedMessage.WRAPPED_FIXED64 << 3 | WireFormat.WIRETYPE_FIXED64:
                value = in.readFixed64();
                break;
            case WrappedMessage.WRAPPED_SFIXED64 << 3 | WireFormat.WIRETYPE_FIXED64:
                value = in.readSFixed64();
                break;
            case WrappedMessage.WRAPPED_INT64 << 3 | WireFormat.WIRETYPE_VARINT:
                value = in.readInt64();
                break;
            case WrappedMessage.WRAPPED_UINT64 << 3 | WireFormat.WIRETYPE_VARINT:
                value = in.readUInt64();
                break;
            case WrappedMessage.WRAPPED_SINT64 << 3 | WireFormat.WIRETYPE_VARINT:
                value = in.readSInt64();
                break;
            case WrappedMessage.WRAPPED_INT32 << 3 | WireFormat.WIRETYPE_VARINT:
                value = in.readInt32();
                break;
            case WrappedMessage.WRAPPED_UINT32 << 3 | WireFormat.WIRETYPE_VARINT:
                value = in.readUInt32();
                break;
            case WrappedMessage.WRAPPED_SINT32 << 3 | WireFormat.WIRETYPE_VARINT:
                value = in.readSInt32();
                break;
            default:
                throw new IllegalStateException("Unexpected tag : " + tag);
            }
        }

        if (value == null && descriptorFullName == null && typeId == null && messageBytes == null) {
            return null;
        }

        if (value != null) {
            if (readTags != 1) {
                throw new IOException("Invalid message encoding.");
            }
            return value;
        }

        if (descriptorFullName == null && typeId == null || descriptorFullName != null && typeId != null
                || readTags != 2) {
            throw new IOException("Invalid message encoding.");
        }

        if (messageBytes != null) {
            // it's a Message type
            RawProtoStreamReader contents = RawProtoStreamReaderImpl.newInstance(messageBytes);
            return TeiidMarshallerContext.getMarsheller().read(contents);
        } else {
            // it's an Enum
            // BaseMarshallerDelegate marshallerDelegate = ((SerializationContextImpl)
            // ctx).getMarshallerDelegate(descriptorFullName);
            // EnumMarshaller marshaller = (EnumMarshaller) marshallerDelegate.getMarshaller();
            // return marshaller.decode(enumValue);
        }
        return null;
    }

    @Override
    protected ByteBuffer objectToBuffer(Object t, int estimatedSize) throws IOException, InterruptedException {
        if (super.isMarshallable(t)) {
            return super.objectToBuffer(t, estimatedSize);
        }

        ByteArrayOutputStreamEx contents = new ByteArrayOutputStreamEx();
        RawProtoStreamWriter out = RawProtoStreamWriterImpl.newInstance(contents);
        TeiidMarshallerContext.getMarsheller().write(t, out);
        String tableName = TeiidMarshallerContext.getMarsheller().getTypeName();
        out.flush();

        ByteArrayOutputStream wrappedContents = new ByteArrayOutputStream();
        RawProtoStreamWriter wrappedOut = RawProtoStreamWriterImpl.newInstance(wrappedContents);
        wrappedOut.writeString(WrappedMessage.WRAPPED_DESCRIPTOR_FULL_NAME, tableName);
        wrappedOut.writeBytes(WrappedMessage.WRAPPED_MESSAGE, contents.getByteBuffer());
        wrappedOut.flush();

        byte[] bytes = wrappedContents.toByteArray();
        return new ByteBufferImpl(bytes, 0, bytes.length);
    }

    public static void writeAttribute(RawProtoStreamWriter out, int tag, Object value) throws IOException {
        if(value == null) {
            return;
        }
        try {
            if (value instanceof String) {
                out.writeString(tag, (String) value);
            } else if (value instanceof Integer) {
                out.writeInt32(tag, (Integer) value);
            } else if (value instanceof Short) {
                out.writeInt32(tag, (Short) value);
            } else if (value instanceof Byte) {
                out.writeInt32(tag, (Byte) value);
            } else if (value instanceof Long) {
                out.writeInt64(tag, (Long) value);
            } else if (value instanceof Boolean) {
                out.writeBool(tag, (Boolean) value);
            } else if (value instanceof Float) {
                out.writeFloat(tag, (Float) value);
            } else if (value instanceof Double) {
                out.writeDouble(tag, (Double) value);
            } else if (value instanceof byte[]) {
                out.writeBytes(tag, (byte[]) value);
            } else if (value instanceof Date) {
                long l = ((Date) value).getTime();
                out.writeBytes(tag, java.nio.ByteBuffer.allocate(Long.SIZE / 8).putLong(l).array());
            } else if (value instanceof Timestamp) {
                long l = ((Timestamp) value).getTime();
                out.writeBytes(tag, java.nio.ByteBuffer.allocate(Long.SIZE / 8).putLong(l).array());
            } else if (value instanceof Time) {
                long l = ((Time) value).getTime();
                out.writeBytes(tag, java.nio.ByteBuffer.allocate(Long.SIZE / 8).putLong(l).array());
            } else if (value instanceof BigInteger) {
                out.writeBytes(tag, ((BigInteger) value).toByteArray());
            } else if (value instanceof BigDecimal) {
                out.writeBytes(tag, ((BigDecimal) value).toBigInteger().toByteArray());
            } else if (value instanceof Clob) {
                Clob clob = (Clob) value;
                out.writeBytes(tag, ObjectConverterUtil.convertToByteArray(clob.getAsciiStream()));
            } else if (value instanceof Blob) {
                Blob blob = (Blob) value;
                out.writeBytes(tag, ObjectConverterUtil.convertToByteArray(blob.getBinaryStream()));
            } else if (value instanceof SQLXML) {
                SQLXML xml = (SQLXML) value;
                out.writeBytes(tag, ObjectConverterUtil.convertToByteArray(xml.getBinaryStream()));
            } else if (value instanceof SQLXML) {
                SQLXML xml = (SQLXML) value;
                out.writeBytes(tag, ObjectConverterUtil.convertToByteArray(xml.getBinaryStream()));
            } else {
                throw new IOException("unknown type, error in Teiid serializer");
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
    /*
    public static Object readAttribute(RawProtoStreamReader in, Class<?> type, int tag) throws IOException {
        Type protoType = Type.STRING;
        if (type.isAssignableFrom(String.class)) {
            protoType = Type.STRING;
            return TeiidMarsheller.getValue(in, protoType, WireFormat.makeTag(tag, protoType.getWireType()));
        } else if (type.isAssignableFrom(Integer.class) || type.isAssignableFrom(Short.class)
                || type.isAssignableFrom(Byte.class)) {
            protoType = Type.INT32;
            return TeiidMarsheller.getValue(in, protoType, WireFormat.makeTag(tag, protoType.getWireType()));
        } else if (type.isAssignableFrom(Long.class)) {
            protoType = Type.INT64;
            return TeiidMarsheller.getValue(in, protoType, WireFormat.makeTag(tag, protoType.getWireType()));
        } else if (type.isAssignableFrom(Boolean.class)) {
            protoType = Type.BOOL;
            return TeiidMarsheller.getValue(in, protoType, WireFormat.makeTag(tag, protoType.getWireType()));
        } else if (type.isAssignableFrom(Float.class)) {
            protoType = Type.FLOAT;
            return TeiidMarsheller.getValue(in, protoType, WireFormat.makeTag(tag, protoType.getWireType()));
        } else if (type.isAssignableFrom(Double.class)) {
            protoType = Type.DOUBLE;
            return TeiidMarsheller.getValue(in, protoType, WireFormat.makeTag(tag, protoType.getWireType()));
        } else if (type.isAssignableFrom(byte[].class)) {
            protoType = Type.BYTES;
            return TeiidMarsheller.getValue(in, protoType, WireFormat.makeTag(tag, protoType.getWireType()));
        } else if (type.isAssignableFrom(Date.class)) {
            protoType = Type.BYTES;
            byte[] value = (byte[]) TeiidMarsheller.getValue(in, protoType,
                    WireFormat.makeTag(tag, protoType.getWireType()));
            if (value == null) {
                return null;
            }
            return new Date(java.nio.ByteBuffer.wrap(value).getLong());
        } else if (type.isAssignableFrom(Timestamp.class)) {
            protoType = Type.BYTES;
            byte[] value = (byte[]) TeiidMarsheller.getValue(in, protoType,
                    WireFormat.makeTag(tag, protoType.getWireType()));
            if (value == null) {
                return null;
            }
            return new Timestamp(java.nio.ByteBuffer.wrap(value).getLong());
        } else if (type.isAssignableFrom(Time.class)) {
            protoType = Type.BYTES;
            byte[] value = (byte[]) TeiidMarsheller.getValue(in, protoType,
                    WireFormat.makeTag(tag, protoType.getWireType()));
            if (value == null) {
                return null;
            }
            return new Time(java.nio.ByteBuffer.wrap(value).getLong());
        } else if (type.isAssignableFrom(BigInteger.class)) {
            protoType = Type.BYTES;
            byte[] value = (byte[]) TeiidMarsheller.getValue(in, protoType,
                    WireFormat.makeTag(tag, protoType.getWireType()));
            if (value == null) {
                return null;
            }
            return new BigInteger(value);
        } else if (type.isAssignableFrom(BigDecimal.class)) {
            protoType = Type.BYTES;
            byte[] value = (byte[]) TeiidMarsheller.getValue(in, protoType,
                    WireFormat.makeTag(tag, protoType.getWireType()));
            if (value == null) {
                return null;
            }
            return new BigDecimal(new BigInteger(value));
        } else if (type.isAssignableFrom(Clob.class)) {
            protoType = Type.BYTES;
            final byte[] value = (byte[]) TeiidMarsheller.getValue(in, protoType,
                    WireFormat.makeTag(tag, protoType.getWireType()));
            if (value == null) {
                return null;
            }
            return new ClobImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new ByteArrayInputStream(value);
                }
            }, -1);
        } else if (type.isAssignableFrom(Blob.class)) {
            protoType = Type.BYTES;
            final byte[] value = (byte[]) TeiidMarsheller.getValue(in, protoType,
                    WireFormat.makeTag(tag, protoType.getWireType()));
            if (value == null) {
                return null;
            }
            return new BlobImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new ByteArrayInputStream(value);
                }
            });
        } else if (type.isAssignableFrom(SQLXML.class)) {
            protoType = Type.BYTES;
            final byte[] value = (byte[]) TeiidMarsheller.getValue(in, protoType,
                    WireFormat.makeTag(tag, protoType.getWireType()));
            if (value == null) {
                return null;
            }
            return new SQLXMLImpl(value);
        } else if(type.isArray()) {
            return readPrimitiveArray(in, type.getComponentType(), tag);
        } else {
            throw new IOException("unknown type, error in Teiid serializer");
        }
    }

    public static Object readPrimitiveArray(RawProtoStreamReader in, Class<?> type, int tag) throws IOException {
        ArrayList<Object> collection = new ArrayList<>();
        Object value = null;

        Type hotrodType = getType(type);

        int expectedTag = WireFormat.makeTag(tag, hotrodType.getWireType());

        while ((value = getValue(in, hotrodType, expectedTag)) != null) {
            collection.add(value);
        }

        if (collection.isEmpty()) {
            return null;
        }

        Object array = Array.newInstance(type, collection.size());
        for (int i = 0; i < collection.size(); i++) {
            Object arrayItem = collection.get(i);
            Array.set(array, i, arrayItem);
        }
        return array;
    }*/

    public static Object getValue(RawProtoStreamReader in, Type type, int expectedTag) throws IOException {
        while (true) {
            int tag = in.readTag();
            if (tag == 0) {
                break;
            }
            if (tag == expectedTag) {
                switch (type) {
                case DOUBLE:
                    return in.readDouble();
                case FLOAT:
                    return in.readFloat();
                case BOOL:
                    return in.readBool();
                case STRING:
                    return in.readString();
                case BYTES:
                    return in.readByteArray();
                case INT32:
                    return in.readInt32();
                case SFIXED32:
                    return in.readSFixed32();
                case FIXED32:
                    return in.readFixed32();
                case UINT32:
                    return in.readUInt32();
                case SINT32:
                    return in.readSInt32();
                case INT64:
                    return in.readInt64();
                case UINT64:
                    return in.readUInt64();
                case FIXED64:
                    return in.readFixed64();
                case SFIXED64:
                    return in.readSFixed64();
                case SINT64:
                    return in.readSInt64();
                default:
                    throw new IOException("Unexpected field type : " + type);
                }
            }
        }
        return null;
    }

}
