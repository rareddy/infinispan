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
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.impl.BaseMarshallerDelegate;

public class TeiidMarsheller extends ProtoStreamMarshaller {
    private SerializationContext ctx;

    public interface Marsheller {
        Object read(RawProtoStreamReader in) throws IOException;

        void write(Object obj, RawProtoStreamWriter out) throws IOException;

        String getTypeName();

        <T> BaseMarshallerDelegate<T> getDelegate();
    }


    public TeiidMarsheller(SerializationContext ctx) {
        this.ctx = ctx;
     }

    @Override
    public SerializationContext getSerializationContext() {
       return this.ctx;
    }

    /*
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
*/
}
