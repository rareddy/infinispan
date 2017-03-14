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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.infinispan.query.dsl.FilterConditionContextQueryBuilder;
import org.infinispan.query.dsl.QueryBuilder;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.TransformationMetadata;

public class TestDSLConversionVisitor {

    private void helpExecute(String query, String expected) throws Exception {
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        System.out.println(ddl);
        helpExecute(mf, query, expected);
    }

    private void helpExecute(MetadataFactory mf, String query, String expected) throws Exception {
        InfinispanExecutionFactory ef = new InfinispanExecutionFactory();
        TransformationMetadata metadata = TestProtobufMetadataProcessor.getTransformationMetadata(mf, ef);
        TranslationUtility utility = new TranslationUtility(metadata);
        Select cmd = (Select)utility.parseCommand(query);

        List<TableReference> tableReference = cmd.getProjectedQuery().getFrom();
        Table table = ((NamedTable)tableReference.get(0)).getMetadataObject();

        DummyQueryFactory factory = new DummyQueryFactory();
        QueryBuilder queryBuilder = factory.from(ProtobufMetadataProcessor.getMessageName(table));
        DSLConvertionVisitor visitor = new DSLConvertionVisitor(new RuntimeMetadataImpl(metadata), factory, queryBuilder);
        visitor.visitNode(cmd);
        QueryBuilder actual = visitor.getTargetQueryBuilder(false);
        assertEquals(expected, actual.toString());
    }
/*
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
        String actual = visitor.getQuery(cmd instanceof Update);
        assertEquals(expected, actual);
    }
    */

    @Test
    public void testSelectStar() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1, e2, e3, e4], "
                + "groupBy=null, "
                + "whereFilterCondition=null, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select * from model.G1", expected);
    }

    @Test
    public void testProjection() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1, e2], "
                + "groupBy=null, "
                + "whereFilterCondition=null, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e1, e2 from model.G1", expected);
    }

    @Test
    public void testAtomaticAddKeyColumn() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e2, e1], "
                + "groupBy=null, "
                + "whereFilterCondition=null, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e2 from model.G1", expected);
    }

    @Test
    public void testEqualityClause() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1, e2, e3, e4], "
                + "groupBy=null, "
                + "whereFilterCondition=AttributeCondition{isNegated=false, expression='e1', operatorAndArgument=EqOperator{argument=1}}, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";
        helpExecute("select * from model.G1 where e1 = 1", expected);
    }

    @Test
    public void testGTEe() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1, e2], "
                + "groupBy=null, "
                + "whereFilterCondition=AttributeCondition{isNegated=false, expression='e1', operatorAndArgument=GteOperator{argument=2}}, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";
        helpExecute("select e1,e2 from model.G1 where e1 >= 2", expected);
    }

    @Test
    public void testInClause() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1], "
                + "groupBy=null, "
                + "whereFilterCondition=AttributeCondition{isNegated=false, expression='e2', operatorAndArgument=InOperator{argument=[foo, bar]}}, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e1 from model.G1 where e2 IN ('foo', 'bar')", expected);
    }

    @Test
    public void testNotInClause() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1], "
                + "groupBy=null, "
                + "whereFilterCondition=NOT (AttributeCondition{isNegated=false, expression='e2', operatorAndArgument=InOperator{argument=[foo, bar]}}, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e1 from model.G1 where e2 NOT IN ('foo', 'bar')", expected);
    }


    @Test
    public void testIsNullClause() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1], "
                + "groupBy=null, "
                + "whereFilterCondition=AttributeCondition{isNegated=false, expression='e2', operatorAndArgument=IsNullOperator{argument=null}}, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e1 from model.G1 where e2 IS NULL", expected);
    }

    @Test
    public void testIsNOTNullClause() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1], "
                + "groupBy=null, "
                + "whereFilterCondition=NOT (AttributeCondition{isNegated=false, expression='e2', operatorAndArgument=IsNullOperator{argument=null}}, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e1 from model.G1 where e2 IS NOT NULL", expected);
    }

    @Test
    public void testLikeClause() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1], "
                + "groupBy=null, "
                + "whereFilterCondition=AttributeCondition{isNegated=false, expression='e2', operatorAndArgument=LikeOperator{argument=%foo%}}, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e1 from model.G1 where e2 like '%foo%'", expected);
    }

    @Test
    public void testNotLikeClause() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1], "
                + "groupBy=null, "
                + "whereFilterCondition=NOT (AttributeCondition{isNegated=false, expression='e2', operatorAndArgument=LikeOperator{argument=%foo%}}, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e1 from model.G1 where e2 NOT like '%foo%'", expected);
    }

    @Test
    public void testOrderBy() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1], "
                + "groupBy=null, "
                + "whereFilterCondition=null, "
                + "havingFilterCondition=null, "
                + "sortCriteria=[SortCriteria{pathExpression='e3', sortOrder=ASC}], "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e1 from model.G1 order by e3", expected);
    }

    @Test
    public void testMultipleOrderBy() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e1], "
                + "groupBy=null, "
                + "whereFilterCondition=null, "
                + "havingFilterCondition=null, "
                + "sortCriteria=[SortCriteria{pathExpression='e1', sortOrder=ASC}, SortCriteria{pathExpression='e3', sortOrder=DESC}], "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e1 from model.G1 order by e1, e3 DESC", expected);
    }

    @Test
    public void testGroupBy() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[SUM(e3), e1], "
                + "groupBy=[e1], "
                + "whereFilterCondition=null, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select sum(e3) from model.G1 group by e1", expected);
    }

    @Test
    public void testGroupByHaving() throws Exception {
        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[SUM(e3), e1], "
                + "groupBy=[e1], "
                + "whereFilterCondition=AttributeCondition{isNegated=false, expression='e2', operatorAndArgument=EqOperator{argument=2}}, "
                + "havingFilterCondition=AttributeCondition{isNegated=false, expression='SUM(e3)', operatorAndArgument=GtOperator{argument=10.0}}, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select sum(e3) from model.G1 where e2 = '2' group by e1 having sum(e3) > 10", expected);
    }

    @Test
    public void testAnd() throws Exception {
        DummyQueryFactory factory = new DummyQueryFactory();
        FilterConditionContextQueryBuilder q = factory.from("foo")
                .having("gender").eq("x")
                .or().having("name").eq("Spider")
                .or().having("gender").eq("y")
                .and().having("surname").like("%oe%");

        String expected = "DummyQueryBuilder [rootTypeName=pm1.G1, "
                + "projection=[e3, e1], "
                + "groupBy=null, "
                + "whereFilterCondition=AttributeCondition{isNegated=false, expression='e1', operatorAndArgument=EqOperator{argument=2}}, "
                + "havingFilterCondition=null, "
                + "sortCriteria=null, "
                + "startOffset=-1, "
                + "maxResults=-1]";

        helpExecute("select e3 from model.G1 where e1 = 2 and e2 < '3'", expected);
    }

    /*
    public void testUpdate() throws Exception {
        helpUpdate("update G1 set e2='bar' where e1 = 1 and e2 = 'foo'",
                "SELECT e1, e2, e3, e4 FROM pm1.G1 WHERE e1 = 1 AND e2 = 'foo'");
    }

    public void testDelete() throws Exception {
        helpUpdate("delete from G1 where e1 > 1 or e2 = 'foo'",
                "SELECT e1 FROM pm1.G1 WHERE e1 > 1 OR e2 = 'foo'");
    }
    */
}
