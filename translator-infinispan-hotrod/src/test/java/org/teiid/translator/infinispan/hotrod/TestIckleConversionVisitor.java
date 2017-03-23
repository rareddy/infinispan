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

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.TransformationMetadata;

public class TestIckleConversionVisitor {

    private IckleConvertionVisitor helpExecute(String query, String expected) throws Exception {
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        System.out.println(ddl);
        return helpExecute(mf, query, expected);
    }

    private IckleConvertionVisitor helpExecute(MetadataFactory mf, String query, String expected) throws Exception {
        InfinispanExecutionFactory ef = new InfinispanExecutionFactory();
        TransformationMetadata metadata = TestProtobufMetadataProcessor.getTransformationMetadata(mf, ef);
        TranslationUtility utility = new TranslationUtility(metadata);
        Select cmd = (Select)utility.parseCommand(query);
        IckleConvertionVisitor visitor = new IckleConvertionVisitor(new RuntimeMetadataImpl(metadata), false);
        visitor.visitNode(cmd);
        String actual = visitor.getQuery();
        assertEquals(expected, actual);
        return visitor;
    }

    private void helpUpdate(String query, String expected) throws Exception {
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        //String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        helpUpdate(mf, query, expected);
    }

    private void helpUpdate(MetadataFactory mf, String query, String expected) throws Exception {
        InfinispanExecutionFactory ef = new InfinispanExecutionFactory();
        TransformationMetadata metadata = TestProtobufMetadataProcessor.getTransformationMetadata(mf, ef);
        TranslationUtility utility = new TranslationUtility(metadata);
        Command cmd = utility.parseCommand(query);
        InfinispanUpdateVisitor visitor = new InfinispanUpdateVisitor(new RuntimeMetadataImpl(metadata));
        visitor.append(cmd);
        String actual = null;
        if (cmd instanceof Update) {
            actual = visitor.getUpdateQuery();
        } else {
            actual = visitor.getDeleteQuery();
        }
        assertEquals(expected, actual);
    }

    @Test
    public void testSelectStar() throws Exception {
        helpExecute("select * from model.G1",
                "SELECT e1, e2, e3, e4, e5 FROM pm1.G1");
    }

    @Test
    public void testProjection() throws Exception {
        helpExecute("select e1, e2 from model.G1",
                "SELECT e1, e2 FROM pm1.G1");
    }

    @Test
    public void testEqualityClause() throws Exception {
        helpExecute("select * from model.G1 where e1 = 1",
                "SELECT e1, e2, e3, e4, e5 FROM pm1.G1 WHERE e1 = 1");
    }

    @Test
    public void testEqualityClauseWithAlias() throws Exception {
        helpExecute("select * from model.G1 as p where p.e1 = 1",
                "SELECT p.e1, p.e2, p.e3, p.e4, p.e5 FROM pm1.G1 p WHERE p.e1 = 1");
    }

    @Test
    public void testInClause() throws Exception {
        helpExecute("select e1, e2 from model.G1 where e2 IN ('foo', 'bar')",
                "SELECT e1, e2 FROM pm1.G1 WHERE e2 IN ('foo', 'bar')");
    }

    @Test
    public void testAggregate() throws Exception {
        helpExecute("select count(*) from model.G1", "SELECT COUNT(*) FROM pm1.G1");
        helpExecute("select sum(e1) from model.G1", "SELECT SUM(e1) FROM pm1.G1");
        helpExecute("select min(e1) from model.G1", "SELECT MIN(e1) FROM pm1.G1");
    }

    @Test
    public void testHaving() throws Exception {
        helpExecute("select sum(e3) from model.G1 where e2 = '2' group by e1 having sum(e3) > 10",
                "SELECT SUM(e3) FROM pm1.G1 WHERE e2 = '2' GROUP BY e1 HAVING SUM(e3) > 10.0");
    }

    @Test
    public void testOrderBy() throws Exception {
        helpExecute("select e1, e2, e3 from model.G1 where e2 IN ('foo', 'bar') order by e3",
                "SELECT e1, e2, e3 FROM pm1.G1 WHERE e2 IN ('foo', 'bar') ORDER BY e3");
    }

    @Test
    public void testUpdate() throws Exception {
        helpUpdate("update G1 set e2='bar' where e1 = 1 and e2 = 'foo'",
                "FROM pm1.G1 __t WHERE __t.e1 = 1 AND __t.e2 = 'foo'");
    }

    @Test
    public void testDelete() throws Exception {
        helpUpdate("delete from G1 where e1 > 1 or e2 = 'foo'",
                "SELECT __t.e1 FROM pm1.G1 __t WHERE __t.e1 > 1 OR __t.e2 = 'foo'");
    }

    @Test
    public void testIsNullClause() throws Exception {
        helpExecute("select e1 from model.G1 where e2 IS NULL", "SELECT e1 FROM pm1.G1 WHERE e2 IS NULL");
        helpExecute("select e1 from model.G1 where e2 IS NOT NULL", "SELECT e1 FROM pm1.G1 WHERE e2 IS NOT NULL");
    }

    @Test
    public void testWithEmbeddedChild() throws Exception {
        helpExecute("select * from model.G2", "FROM pm1.G2");
        helpExecute("select * from model.G2 as p", "FROM pm1.G2 p");
        helpExecute("select * from model.G2 as p where g3_e1 = 2",
                "FROM pm1.G2 p WHERE p.g3.e1 = 2");
    }

    @Test
    public void testWithExternalChild() throws Exception {
        helpExecute("select * from model.G4", "FROM pm1.G2");
        helpExecute("select * from model.G4 as p", "FROM pm1.G2 p");
        helpExecute("select * from model.G4 where G2_e1 = 2", "FROM pm1.G2 WHERE e1 = 2");
        IckleConvertionVisitor visitor = helpExecute("select * from model.G4 as p where p.G2_e1 = 2",
                "FROM pm1.G2 p WHERE p.e1 = 2");
        assertArrayEquals(new String[] { "pm1.G2/pm1.G4/e1", "pm1.G2/pm1.G4/e2" },
                visitor.getProjectedDocumentAttributes().toArray(new String[2]));
    }

    @Test
    public void testJoins() throws Exception {
        helpExecute("select g2.e1, g4.e1 from model.G2 g2 JOIN model.G4 g4 ON g2.e1 = g4.g2_e1",
                "FROM pm1.G2 g2"); // where is not generated because both columns as one and same

        helpExecute("select g2.e1, g4.e1 from model.G2 g2 JOIN model.G4 g4 ON g2.e1 = g4.g2_e1 "
                + "WHERE g2.e2 = 'foo' AND g4.e2 = 'bar'",
                "FROM pm1.G2 g2 WHERE g2.e2 = 'foo' AND g2.g4.e2 = 'bar'");

    }
}
