package org.teiid.translator.infinispan.hotrod;


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
import org.teiid.infinispan.api.ProtobufDataManager;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
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
            display="Message Name",
            description="Message name this table or column represents")
    public static final String MESSAGE_NAME = MetadataFactory.ODATA_URI+"MESSAGE_NAME"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable= Column.class,
            datatype=String.class,
            display="Protobuf Tag Number",
            description="Protobuf field tag number")
    public static final String TAG = MetadataFactory.ODATA_URI+"TAG"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable = {Table.class, Column.class},
            datatype=String.class,
            display="Protobuf Parent Tag Number",
            description="Protobuf field parent tag number in the case of complex document")
    public static final String PARENT_TAG = MetadataFactory.ODATA_URI+"PARENT_TAG"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable= Column.class,
            datatype=String.class,
            display="column's parent column name",
            description="Protobuf field parent column name in the case of complex document")
    public static final String PARENT_COLUMN_NAME = MetadataFactory.ODATA_URI+"PARENT_COLUMN_NAME"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class,
            datatype=String.class,
            display="Pseudo Column",
            description="Pseudo column for join purposes")
    public static final String PSEUDO = MetadataFactory.ODATA_URI+"PSEUDO"; //$NON-NLS-1$

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
            toTeiidSchema(protobufFile, protoContents, metadataFactory);
        } else {
            // Read from cache
            BasicCache<Object, Object> metadataCache = connection
                    .getCacheFactory().getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
            for (Object key : metadataCache.keySet()) {
                if (WRAPPING_DEFINITIONS_RES.equals(key)) {
                    continue;
                }
                protobufFile = (String)key;
                protoContents = (String)metadataCache.get(key);
                // read all the schemas
                toTeiidSchema(protobufFile, protoContents, metadataFactory);
            }
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

        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Processing Proto file:", name, "  with contents\n", contents);

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
        table.setProperty(MESSAGE_NAME, messageElement.qualifiedName());
        table.setAnnotation(messageElement.documentation());

        for (FieldElement fieldElement:messageElement.fields()) {
            addColumn(mf, messageTypes, enumTypes, columnPrefix, table, fieldElement, ignoreTables, false);
        }
        return table;
    }

    private Column addColumn(MetadataFactory mf,
            List<MessageElement> messageTypes, List<EnumElement> enumTypes,
            String parentTableColumn, Table table,
            FieldElement fieldElement, HashSet<String> ignoreTables, boolean nested) throws TranslatorException {

        DataType type = fieldElement.type();
        String annotation = fieldElement.documentation();
        String columnName =  fieldElement.name();

        String teiidType = null;
        if (isEnum(messageTypes, enumTypes, type)) {
            teiidType = ProtobufDataManager.teiidType(type, isCollection(fieldElement), true);
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
                Table nestedTable = addTable(mf, messageTypes, enumTypes, nestedMessageElement,
                        parentTableColumn == null ? columnName : parentTableColumn + Tokens.DOT + columnName,
                        ignoreTables);
                if (table.getPrimaryKey() != null) {
                    // add additional column to represent the relationship
                    Column parentColumn = table.getPrimaryKey().getColumns().get(0);
                    String psedoColumnName = table.getName()+"_"+parentColumn.getName();
                    Column addedColumn = mf.addColumn(psedoColumnName, parentColumn.getRuntimeType(), nestedTable);
                    addedColumn.setNameInSource(parentColumn.getName());
                    addedColumn.setSelectable(false);
                    addedColumn.setProperty(PSEUDO, columnName);
                    List<String> keyColumns = new ArrayList<String>();
                    keyColumns.add(addedColumn.getName());
                    List<String> refColumns = new ArrayList<String>();
                    refColumns.add(parentColumn.getName());
                    mf.addForiegnKey("FK_"+table.getName().toUpperCase(), keyColumns, refColumns, table.getName(), nestedTable); //$NON-NLS-1$

                    // since this nested table can not be reached directly, put a access
                    // pattern on it.
                    mf.addAccessPattern("AP_"+addedColumn.getName().toUpperCase(), Arrays.asList(addedColumn.getName()), nestedTable);
                    nestedTable.setProperty(MERGE, table.getFullName());
                    nestedTable.setProperty(PARENT_TAG, Integer.toString(fieldElement.tag()));
                    nestedTable.setProperty(PARENT_COLUMN_NAME, columnName);
                } else {
                    ignoreTables.add(nestedName);
                    LogManager.logInfo(LogConstants.CTX_CONNECTOR,
                            InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25006, nestedName));
                }
            } else {
                ignoreTables.add(nestedMessageElement.name());
                // inline all the columns from the message and return
                for (FieldElement nestedElement:nestedMessageElement.fields()) {
                    Column nestedColumn = addColumn(mf, messageTypes, enumTypes, columnName, table, nestedElement, ignoreTables, true);
                    nestedColumn.setNameInSource(nestedElement.name());
                    nestedColumn.setProperty(MESSAGE_NAME, nestedMessageElement.qualifiedName());
                    nestedColumn.setProperty(PARENT_TAG, Integer.toString(fieldElement.tag()));
                    nestedColumn.setProperty(PARENT_COLUMN_NAME, columnName);
                }
            }
            return null;
        } else {
            teiidType = ProtobufDataManager.teiidType(type, isCollection(fieldElement), false);
        }

        Column c = null;
        if (nested) {
            c = mf.addColumn(parentTableColumn + "_" + columnName, teiidType, table);
        } else {
            c = mf.addColumn(columnName, teiidType, table);
        }
        c.setNativeType(fieldElement.type().toString());
        c.setUpdatable(true);
        c.setNullType(fieldElement.label() == Label.REQUIRED ? NullType.No_Nulls : NullType.Nullable);
        c.setProperty(TAG, Integer.toString(fieldElement.tag()));

        // process default value
        if (fieldElement.getDefault() != null) {
            if (isEnum(messageTypes, enumTypes, type)) {
                String ordinal = getEnumOrdinal(messageTypes, enumTypes,((DataType.NamedType) type).name(),
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

    private String getEnumOrdinal(List<MessageElement> messageTypes, List<EnumElement> enumTypes, String name, String value) {
        for (EnumElement element:enumTypes) {
            if (element.name().equals(name)) {
                for(EnumConstantElement constant:element.constants()) {
                    if (constant.name().equals(value)) {
                        return String.valueOf(constant.tag());
                    }
                }
            }
        }

        // enum does not nest, messages nest
        for (MessageElement element:messageTypes) {
            List<MessageElement> childMessageTypes = filter(element.nestedElements(), MessageElement.class);
            List<EnumElement> childEnumTypes = filter(element.nestedElements(), EnumElement.class);
            String child = getEnumOrdinal(childMessageTypes, childEnumTypes, name, value);
            if (child != null) {
                return child;
            }
        }
        return null;
    }

    private boolean isEnum(List<MessageElement> messageTypes, List<EnumElement> enumTypes, DataType type) {
        if (type instanceof DataType.NamedType) {
            for (EnumElement element:enumTypes) {
                if (element.name().equals(((DataType.NamedType)type).name())) {
                    return true;
                }

            }
            // enum does not nest, messages nest
            for (MessageElement element:messageTypes) {
                List<MessageElement> childMessageTypes = filter(element.nestedElements(), MessageElement.class);
                List<EnumElement> childEnumTypes = filter(element.nestedElements(), EnumElement.class);
                if (isEnum(childMessageTypes, childEnumTypes, type)) {
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

                // check also nested messages
                List<MessageElement> childMessageTypes = filter(element.nestedElements(), MessageElement.class);
                if (isMessage(childMessageTypes, type)) {
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
            List<MessageElement> childMessageTypes = filter(element.nestedElements(), MessageElement.class);
            MessageElement child = getMessage(childMessageTypes, name);
            if (child != null) {
                return child;
            }
        }
        return null;
    }

    public ProtobufResource getProtobufResource() {
        return this.protoResource;
    }

    static String getPseudo(Column column) {
        return column.getProperty(PSEUDO, false);
    }

    static String getMessageName(Table table) {
        return table.getProperty(MESSAGE_NAME, false);
    }

    static String getMessageName(Column column) {
        return column.getProperty(MESSAGE_NAME, false);
    }

    static String getMerge(Table table) {
        return table.getProperty(MERGE, false);
    }

    static int getTag(Column column) {
        return Integer.parseInt(column.getProperty(TAG, false));
    }

    static int getParentTag(Column column) {
        if (column.getProperty(PARENT_TAG, false) != null) {
            return Integer.parseInt(column.getProperty(PARENT_TAG, false));
        }
        return -1;
    }

    static int getParentTag(Table table) {
        if (table.getProperty(PARENT_TAG, false) != null) {
            return Integer.parseInt(table.getProperty(PARENT_TAG, false));
        }
        return -1;
    }

    static String getParentColumnName(Column column) {
        return column.getProperty(PARENT_COLUMN_NAME, false);
    }
}
