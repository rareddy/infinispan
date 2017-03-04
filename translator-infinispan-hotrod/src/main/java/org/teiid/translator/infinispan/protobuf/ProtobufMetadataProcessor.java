package org.teiid.translator.infinispan.protobuf;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;

import infinispan.com.squareup.protoparser.DataType;
import infinispan.com.squareup.protoparser.EnumConstantElement;
import infinispan.com.squareup.protoparser.EnumElement;
import infinispan.com.squareup.protoparser.FieldElement;
import infinispan.com.squareup.protoparser.FieldElement.Label;
import infinispan.com.squareup.protoparser.MessageElement;
import infinispan.com.squareup.protoparser.ProtoFile;
import infinispan.com.squareup.protoparser.ProtoParser;
import infinispan.com.squareup.protoparser.TypeElement;


public class ProtobufMetadataProcessor implements MetadataProcessor<InfinispanConnection> {
    private static final String WRAPPING_DEFINITIONS_RES = "/org/infinispan/protostream/message-wrapping.proto";
    
    @ExtensionMetadataProperty(applicable=Table.class,
            datatype=String.class, 
            display="Merge Into Table", 
            description="Declare the name of parent table that this table needs to be merged into.")
    public static final String MERGE = MetadataFactory.ODATA_URI+"MERGE"; //$NON-NLS-1$
    
    @ExtensionMetadataProperty(applicable=Table.class,
            datatype=String.class, 
            display="Cache Name", 
            description="Cache name to store the contents into")
    public static final String CACHE = MetadataFactory.ODATA_URI+"CACHE"; //$NON-NLS-1$
    
    @ExtensionMetadataProperty(applicable= {Table.class,Column.class}, 
            datatype=String.class, 
            display="Message", 
            description="Message name this table or column represnts")
    public static final String MESSAGE = MetadataFactory.ODATA_URI+"MESSAGE"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class, 
            datatype=String.class, 
            display="Protobuf Tag Number", 
            description="Protobuf field tag number")
    public static final String TAG = MetadataFactory.ODATA_URI+"TAG"; //$NON-NLS-1$    
    
    private String protoFilePath;
    private ProtobufResource protoResource;
    
    @TranslatorProperty(display="Protobuf file path", category=PropertyType.IMPORT, 
            description="Protobuf metadata file path.")
    public String getProtoFilePath() {
        return protoFilePath;
    }

    public void setProtoFilePath(String path) {
        this.protoFilePath = path;
    }       
    
    @Override
    public void process(MetadataFactory metadataFactory, InfinispanConnection connection)
            throws TranslatorException {
        
        String protobufFile = getProtoFilePath();
        String protoContents = null; 
        if( protobufFile != null &&  !protobufFile.isEmpty()) {                
            File f = new File(protobufFile);
            if(f == null || !f.exists() || !f.isFile()) {
                throw new TranslatorException(InfinispanPlugin.Event.TEIID25000,
                        InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25000, protobufFile));
            }
            try {
                protoContents = ObjectConverterUtil.convertFileToString(f);
            } catch (IOException e) {
                throw new TranslatorException(e);
            }
            this.protoResource = new ProtobufResource(protobufFile, protoContents);
        } else {
            // Read from cache
            BasicCache<Object, Object> metadataCache = connection
                    .getCacheFactory().getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
            for (Object key : metadataCache.keySet()) {
                if (WRAPPING_DEFINITIONS_RES.equals((String)key)) {
                    continue;
                }
                protobufFile = (String)key;
                protoContents = (String)metadataCache.get((String)key);
                break;
            }
        }
                
        if (protoContents != null) {
            toTeiidSchema(protobufFile, protoContents, metadataFactory);
        }
    }
    
    @SuppressWarnings(value = "unchecked")
    static <T> List<T> filter(List<? super TypeElement> input, Class<T> ofType) {
       List<T> ts = new LinkedList<>();
       for (Object elem : input) {
          if (ofType.isAssignableFrom(elem.getClass())) {
             ts.add((T) elem);
          }
       }
       return ts;
    }    
    
    private void toTeiidSchema(String name, String contents,
            MetadataFactory mf) throws TranslatorException {
        ProtoFile protoFile = ProtoParser.parse(name, contents);
        
        List<MessageElement> messageTypes = filter(protoFile.typeElements(), MessageElement.class);
        List<EnumElement> enumTypes = filter(protoFile.typeElements(), EnumElement.class);

        // add tables
        HashSet<String> deleteTables = new HashSet<>();
        for (MessageElement messageElement:messageTypes) {
            addTable(mf, messageTypes, enumTypes, messageElement, null, deleteTables);
        }
        
        for (String tableName:deleteTables) {
            mf.getSchema().removeTable(tableName);
        }
    }

    private Table addTable(MetadataFactory mf,
            List<MessageElement> messageTypes, List<EnumElement> enumTypes,
            MessageElement messageElement, String columnPrefix,
            HashSet<String> ignoreTables) throws TranslatorException {
        
        String tableName = messageElement.name();
        if (mf.getSchema().getTable(tableName) != null) {
            return mf.getSchema().getTable(tableName);
        }
        
        if (ignoreTables.contains(tableName)) {
            return null;
        }
        
        Table table = mf.addTable(tableName);
        table.setSupportsUpdate(true);
        table.setProperty(MESSAGE, messageElement.qualifiedName());
        table.setAnnotation(messageElement.documentation());
        
        for (FieldElement fieldElement:messageElement.fields()) {
            addColumn(mf, messageTypes, enumTypes, columnPrefix, table, fieldElement, ignoreTables);
        }
        return table;
    }

    private Column addColumn(MetadataFactory mf,
            List<MessageElement> messageTypes, List<EnumElement> enumTypes,
            String columnPrefix, Table table,
            FieldElement fieldElement, HashSet<String> ignoreTables) throws TranslatorException {
        
        DataType type = fieldElement.type();
        String annotation = fieldElement.documentation();
        String columnName =  fieldElement.name();
        if (columnPrefix != null) {
            columnName = columnPrefix+"_"+columnName;
        }
        
        String teiidType = null;
        if (isEnum(enumTypes, type)) {
            teiidType = ProtoTypeManager.teiidType(type, isCollection(fieldElement), true);
        } else if (isMessage(messageTypes, type)) {
            // this is nested table. If the nested table has PK, then we will configure external
            // if not we will consider this as embedded with primary table.
            String nestedName = ((DataType.NamedType)type).name();
            MessageElement nestedMessageElement = getMessage(messageTypes, nestedName);
            if (nestedMessageElement == null) {
                throw new TranslatorException(InfinispanPlugin.Event.TEIID25001,
                        InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25001, nestedName, columnName));
            }
            
            // this is one-2-many
            if (isCollection(fieldElement)) {
                Table nestedTable = addTable(mf, messageTypes, enumTypes, nestedMessageElement, columnName, ignoreTables);

                // add additional column to represent the relationship
                Column parentColumn = table.getPrimaryKey().getColumns().get(0);
                String psedoColumnName = table.getName()+"_"+columnName; 
                Column addedColumn = mf.addColumn(psedoColumnName, parentColumn.getRuntimeType(), nestedTable);
                addedColumn.setSelectable(false);
                List<String> keyColumns = new ArrayList<String>();
                keyColumns.add(addedColumn.getName());
                List<String> refColumns = new ArrayList<String>();
                refColumns.add(addedColumn.getName());
                mf.addForiegnKey("FK_"+table.getName().toUpperCase(), keyColumns, refColumns, table.getName(), nestedTable); //$NON-NLS-1$

                // since this nested table can not be reached directly, put a access
                // pattern on it.
                if (nestedTable.getPrimaryKey() == null) {
                    mf.addAccessPattern("AP_"+addedColumn.getName().toUpperCase(), Arrays.asList(addedColumn.getName()), nestedTable);
                    nestedTable.setProperty(MERGE, table.getName());
                }
            } else {
                ignoreTables.add(nestedMessageElement.name());
                // inline all the columns from the message and return
                for (FieldElement nestedElement:nestedMessageElement.fields()) {
                    Column nestedColumn = addColumn(mf, messageTypes, enumTypes, columnName, table, nestedElement, ignoreTables);
                    nestedColumn.setNameInSource(nestedElement.name());
                    nestedColumn.setProperty(MESSAGE, nestedMessageElement.qualifiedName());
                }
            }
            return null;
        } else {
            teiidType = ProtoTypeManager.teiidType(type, isCollection(fieldElement), false);
        }
        
        Column c = mf.addColumn(columnName, teiidType, table);
        c.setUpdatable(true);
        c.setNullType(fieldElement.label() == Label.REQUIRED ? NullType.No_Nulls : NullType.Nullable);
        c.setProperty(TAG, Integer.toString(fieldElement.tag()));
        
        // process default value
        if (fieldElement.getDefault() != null) {
            if (isEnum(enumTypes, type)) {
                String ordinal = getEnumOrdinal(enumTypes,((DataType.NamedType) type).name(),
                        fieldElement.getDefault().value().toString());
                if (ordinal != null) {
                    c.setDefaultValue(ordinal);
                }
            } else {
                c.setDefaultValue(fieldElement.getDefault().value().toString());
            }
        }
        
        // process annotations
        if ( annotation != null && !annotation.isEmpty()) {
            c.setAnnotation(annotation);
            
            if(annotation.contains("@IndexedField")) {
                c.setSearchType(SearchType.Searchable);
            }
            
            if(annotation.contains("@Id")) {
                List<String> pkNames = new ArrayList<String>();
                pkNames.add(fieldElement.name());
                mf.addPrimaryKey("PK_"+fieldElement.name().toUpperCase(), pkNames, table);
            }
        }
        return c;
    }

    private boolean isCollection(FieldElement fieldElement) {
        return fieldElement.label() == Label.REPEATED;
    }
    
    private String getEnumOrdinal(List<EnumElement> enumTypes, String name, String value) {
        for (EnumElement enumElement:enumTypes) {
            if (enumElement.name().equals(name)) {
                for(EnumConstantElement constant:enumElement.constants()) {
                    if (constant.name().equals(value)) {
                        return String.valueOf(constant.tag());
                    }
                }
            }
        }
        return null;
    }
    
    private boolean isEnum(List<EnumElement> enumTypes, DataType type) {
        if (type instanceof DataType.NamedType) {
            for (EnumElement element:enumTypes) {
                if (element.name().equals(((DataType.NamedType)type).name())) {
                    return true;
                }
            }
        }
        return false;        
    }
    
    private boolean isMessage(List<MessageElement> messageTypes, DataType type) {
        if (type instanceof DataType.NamedType) {
            for (MessageElement element:messageTypes) {
                if (element.name().equals(((DataType.NamedType)type).name())) {
                    return true;
                }
            }
        }
        return false;        
    }
    
    private MessageElement getMessage(List<MessageElement> messageTypes, String name) {
        for (MessageElement element : messageTypes) {
            if (element.name().equals(name)) {
                return element;
            }
        }
        return null;
    }
    
    public ProtobufResource getProtobufResource() {
        return this.protoResource;
    }
}
