SET NAMESPACE 'http://www.jboss.org/teiiddesigner/ext/odata/2012' AS teiid_odata;

CREATE FOREIGN TABLE Person (
	name string NOT NULL OPTIONS (ANNOTATION '@IndexedField', SEARCHABLE 'Searchable', "teiid_odata:TAG" '1'),
	id integer NOT NULL OPTIONS (ANNOTATION '@Id\u000A@IndexedField(index=true, store=false)', SEARCHABLE 'Searchable', "teiid_odata:TAG" '2'),
	email string OPTIONS ("teiid_odata:TAG" '3'),
	address_Address string NOT NULL OPTIONS (ANNOTATION '@IndexedField', NAMEINSOURCE 'Address', SEARCHABLE 'Searchable', "teiid_odata:MESSAGE" 'quickstart.Address', "teiid_odata:TAG" '1'),
	address_City string NOT NULL OPTIONS (ANNOTATION '@IndexedField(index=true, store=false)', NAMEINSOURCE 'City', SEARCHABLE 'Searchable', "teiid_odata:MESSAGE" 'quickstart.Address', "teiid_odata:TAG" '2'),
	address_State string NOT NULL OPTIONS (ANNOTATION '@IndexedField(index=true, store=false)', NAMEINSOURCE 'State', SEARCHABLE 'Searchable', "teiid_odata:MESSAGE" 'quickstart.Address', "teiid_odata:TAG" '3'),
	CONSTRAINT PK_ID PRIMARY KEY(id)
) OPTIONS (ANNOTATION '@Indexed', UPDATABLE TRUE, "teiid_odata:MESSAGE" 'quickstart.Person');

CREATE FOREIGN TABLE PhoneNumber (
	phone_number string NOT NULL OPTIONS (ANNOTATION '@IndexedField', SEARCHABLE 'Searchable', "teiid_odata:TAG" '1'),
	phone_type integer DEFAULT '1' OPTIONS (ANNOTATION '@IndexedField(index=false, store=false)', SEARCHABLE 'Searchable', "teiid_odata:TAG" '2'),
	Person_phone integer OPTIONS (SELECTABLE FALSE, UPDATABLE FALSE),
	CONSTRAINT AP_PERSON_PHONE ACCESSPATTERN(Person_phone),
	CONSTRAINT FK_PERSON FOREIGN KEY(Person_phone) REFERENCES Person (Person_phone)
) OPTIONS (ANNOTATION '@Indexed', UPDATABLE TRUE, "teiid_odata:MERGE" 'Person', "teiid_odata:MESSAGE" 'quickstart.PhoneNumber');