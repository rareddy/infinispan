SET NAMESPACE 'http://www.jboss.org/teiiddesigner/ext/odata/2012' AS teiid_odata;

CREATE FOREIGN TABLE Person (
	name string NOT NULL OPTIONS (ANNOTATION '@IndexedField', SEARCHABLE 'Searchable', "teiid_odata:TAG" '1'),
	id integer NOT NULL OPTIONS (ANNOTATION '@Id\u000A@IndexedField(index=false, store=false)', SEARCHABLE 'Searchable', "teiid_odata:TAG" '2'),
	email string OPTIONS ("teiid_odata:TAG" '3'),
	CONSTRAINT PK_ID PRIMARY KEY(id)
) OPTIONS (ANNOTATION '@Indexed', UPDATABLE TRUE, "teiid_odata:MESSAGE_NAME" 'quickstart.Person');

CREATE FOREIGN TABLE PhoneNumber (
	phone_number string NOT NULL OPTIONS (ANNOTATION '@IndexedField', NAMEINSOURCE 'phone.number', SEARCHABLE 'Searchable', "teiid_odata:TAG" '1'),
	phone_type integer DEFAULT '1' OPTIONS (ANNOTATION '@IndexedField(index=false, store=false)', NAMEINSOURCE 'phone.type', SEARCHABLE 'Searchable', "teiid_odata:TAG" '2'),
	Person_id integer OPTIONS (NAMEINSOURCE 'id', SELECTABLE FALSE, UPDATABLE FALSE, "teiid_odata:PSEUDO" 'phone'),
	CONSTRAINT AP_PERSON_ID ACCESSPATTERN(Person_id),
	CONSTRAINT FK_PERSON FOREIGN KEY(Person_id) REFERENCES Person (id)
) OPTIONS (ANNOTATION '@Indexed', UPDATABLE TRUE, "teiid_odata:MERGE" 'model.Person', "teiid_odata:MESSAGE_NAME" 'quickstart.Person.PhoneNumber');