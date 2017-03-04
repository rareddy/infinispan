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
package org.teiid.translator.infinispan.protobuf;

import java.util.HashMap;

import org.teiid.core.types.DataTypeManager;

import infinispan.com.squareup.protoparser.DataType;
import infinispan.com.squareup.protoparser.DataType.ScalarType;

public class ProtoTypeManager {

    private static HashMap<ScalarType, String> protoTypes = new HashMap<ScalarType, String>();
    private static HashMap<String, ScalarType> teiidTypes = new HashMap<String, ScalarType>();

    static {
        protoTypes.put(ScalarType.STRING, DataTypeManager.DefaultDataTypes.STRING);
        protoTypes.put(ScalarType.BOOL, DataTypeManager.DefaultDataTypes.BOOLEAN);
        
        protoTypes.put(ScalarType.FIXED32, DataTypeManager.DefaultDataTypes.INTEGER);
        protoTypes.put(ScalarType.INT32, DataTypeManager.DefaultDataTypes.INTEGER);
        protoTypes.put(ScalarType.SINT32, DataTypeManager.DefaultDataTypes.INTEGER);
        protoTypes.put(ScalarType.UINT32, DataTypeManager.DefaultDataTypes.INTEGER);
        
        protoTypes.put(ScalarType.FIXED64, DataTypeManager.DefaultDataTypes.LONG);
        protoTypes.put(ScalarType.INT64, DataTypeManager.DefaultDataTypes.LONG);
        protoTypes.put(ScalarType.SINT64, DataTypeManager.DefaultDataTypes.LONG);
        protoTypes.put(ScalarType.UINT64, DataTypeManager.DefaultDataTypes.LONG);
        
        protoTypes.put(ScalarType.FLOAT, DataTypeManager.DefaultDataTypes.FLOAT);
        protoTypes.put(ScalarType.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE);
        protoTypes.put(ScalarType.BYTES, DataTypeManager.DefaultDataTypes.VARBINARY); //$NON-NLS-1$
        
//        odataTypes.put("Edm.Date", DataTypeManager.DefaultDataTypes.DATE);
//        odataTypes.put("Edm.TimeOfDay", DataTypeManager.DefaultDataTypes.TIME);
//        odataTypes.put("Edm.DateTimeOffset", DataTypeManager.DefaultDataTypes.TIMESTAMP);
        
        teiidTypes.put(DataTypeManager.DefaultDataTypes.STRING, ScalarType.STRING);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BOOLEAN, ScalarType.BOOL);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.SHORT, ScalarType.INT32);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BYTE, ScalarType.INT32);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.SHORT, ScalarType.INT32);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.INTEGER, ScalarType.INT32);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.LONG, ScalarType.INT64);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.FLOAT, ScalarType.FLOAT);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DOUBLE, ScalarType.DOUBLE);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, ScalarType.BYTES);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DATE, ScalarType.STRING);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIME, ScalarType.STRING);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, ScalarType.STRING);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BLOB, ScalarType.BYTES);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.CLOB, ScalarType.BYTES);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.XML, ScalarType.BYTES);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.VARBINARY, ScalarType.BYTES); //$NON-NLS-1$
        //will fail for most values
        teiidTypes.put(DataTypeManager.DefaultDataTypes.OBJECT, ScalarType.BYTES); //$NON-NLS-1$ 
        teiidTypes.put(DataTypeManager.DefaultDataTypes.GEOMETRY, ScalarType.BYTES); //$NON-NLS-1$
    }
    
    public static String teiidType(DataType protoType, boolean array, boolean isEnum) {
        // treat all enums as integers
        if (isEnum) {
            return DataTypeManager.DefaultDataTypes.INTEGER;
        }
        
        String type =  protoTypes.get(protoType);
        if (type == null) {
            type = DataTypeManager.DefaultDataTypes.STRING; // special case for enum type
        }
        if (array) {
           type +="[]";
        }
        return type;
    }
}
