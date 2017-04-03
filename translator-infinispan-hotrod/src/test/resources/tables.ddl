SET NAMESPACE 'http://www.jboss.org/teiiddesigner/ext/odata/2012' AS teiid_odata;

CREATE FOREIGN TABLE G1 (
	e1 integer NOT NULL OPTIONS (ANNOTATION '@Id\u000A@IndexedField(index=true, store=false)', SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_odata:TAG" '1'),
	e2 string NOT NULL OPTIONS (ANNOTATION '@IndexedField', SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_odata:TAG" '2'),
	e3 float OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'float', "teiid_odata:TAG" '3'),
	e4 string[] OPTIONS (ANNOTATION '@IndexedField(index=true, store=false)', SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_odata:TAG" '4'),
	e5 string[] OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_odata:TAG" '5'),
	CONSTRAINT PK_E1 PRIMARY KEY(e1)
) OPTIONS (ANNOTATION '@Indexed', NAMEINSOURCE 'pm1.G1', UPDATABLE TRUE);

CREATE FOREIGN TABLE G2 (
	e1 integer NOT NULL OPTIONS (ANNOTATION '@Id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_odata:TAG" '1'),
	e2 string NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_odata:TAG" '2'),
	g3_e1 integer NOT NULL OPTIONS (NAMEINSOURCE 'e1', SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_odata:MESSAGE_NAME" 'pm1.G3', "teiid_odata:PARENT_COLUMN_NAME" 'g3', "teiid_odata:PARENT_TAG" '5', "teiid_odata:TAG" '1'),
	g3_e2 string NOT NULL OPTIONS (NAMEINSOURCE 'e2', SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_odata:MESSAGE_NAME" 'pm1.G3', "teiid_odata:PARENT_COLUMN_NAME" 'g3', "teiid_odata:PARENT_TAG" '5', "teiid_odata:TAG" '2'),
	e5 varbinary OPTIONS (ANNOTATION '@IndexedField(index=false)', NATIVE_TYPE 'bytes', "teiid_odata:TAG" '7'),
	e6 long OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'fixed64', "teiid_odata:TAG" '8'),
	CONSTRAINT PK_E1 PRIMARY KEY(e1)
) OPTIONS (ANNOTATION '@Indexed', NAMEINSOURCE 'pm1.G2', UPDATABLE TRUE);

CREATE FOREIGN TABLE G4 (
	e1 integer NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_odata:TAG" '1'),
	e2 string NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_odata:TAG" '2'),
	G2_e1 integer OPTIONS (NAMEINSOURCE 'e1', SEARCHABLE 'Searchable', "teiid_odata:PSEUDO" 'g4'),
	CONSTRAINT FK_G2 FOREIGN KEY(G2_e1) REFERENCES G2 (e1)
) OPTIONS (ANNOTATION '@Indexed', NAMEINSOURCE 'pm1.G4', UPDATABLE TRUE, "teiid_odata:MERGE" 'model.G2', "teiid_odata:PARENT_COLUMN_NAME" 'g4', "teiid_odata:PARENT_TAG" '6');