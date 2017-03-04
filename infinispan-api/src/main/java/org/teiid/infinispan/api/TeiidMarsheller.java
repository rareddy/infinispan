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

import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.protostream.impl.ByteArrayOutputStreamEx;
import org.infinispan.protostream.impl.RawProtoStreamReaderImpl;
import org.infinispan.protostream.impl.RawProtoStreamWriterImpl;
import org.infinispan.protostream.impl.WireFormat;

public class TeiidMarsheller extends ProtoStreamMarshaller {
    
    static Object getValue(RawProtoStreamReader in, Type type, int expectedTag) throws IOException {
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

    @Override
    public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(buf, offset, length);
        RawProtoStreamReader reader = RawProtoStreamReaderImpl.newInstance(bais);
        return readMessage(reader);
    }

    @Override
    public boolean isMarshallable(Object o) {
        return true;
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
              case WrappedMessage.WRAPPED_MESSAGE << 3 | WireFormat.WIRETYPE_LENGTH_DELIMITED:
                 messageBytes = in.readByteArray();
                 break;
              default:
                 throw new IllegalStateException("Unexpected tag : " + tag);
           }
        }

        if (value == null && descriptorFullName == null && typeId == null && messageBytes == null) {
            return null;
        }

        if (descriptorFullName == null && typeId == null || descriptorFullName != null && typeId != null || readTags != 2) {
           throw new IOException("Invalid message encoding.");
        }

        if (messageBytes != null) {
           // it's a Message type
           RawProtoStreamReader contents = RawProtoStreamReaderImpl.newInstance(messageBytes);
           return TeiidMarshallerContext.getMarsheller().read(descriptorFullName, contents);
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
        String tableName = TeiidMarshallerContext.getMarsheller().write(t, out);
        out.flush();
        
        ByteArrayOutputStream wrappedContents = new ByteArrayOutputStream();        
        RawProtoStreamWriter wrappedOut = RawProtoStreamWriterImpl.newInstance(wrappedContents);
        wrappedOut.writeString(WrappedMessage.WRAPPED_DESCRIPTOR_FULL_NAME, tableName);
        wrappedOut.writeBytes(WrappedMessage.WRAPPED_MESSAGE, contents.getByteBuffer());
        wrappedOut.flush();
        
        byte[] bytes = wrappedContents.toByteArray();
        return new ByteBufferImpl(bytes, 0, bytes.length);
    }
}
