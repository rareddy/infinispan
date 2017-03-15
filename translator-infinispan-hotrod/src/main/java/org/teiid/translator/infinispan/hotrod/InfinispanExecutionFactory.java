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

import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.core.util.PropertiesUtils;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;
import org.teiid.util.Version;


@Translator(name = "infinispan-hotrod", description = "The Infinispan Translator Using Protobuf & Hotrod")
public class InfinispanExecutionFactory extends ExecutionFactory<ConnectionFactory, InfinispanConnection>{
    public static final Version SIX_6 = Version.getVersion("6.6"); //$NON-NLS-1$
	public static final int MAX_SET_SIZE = 1024;

	private boolean supportsCompareCriteriaOrdered = false;
	private boolean supportsUpsert = true;
	private ProtobufResource protobuf;

	public InfinispanExecutionFactory() {
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(MAX_SET_SIZE);
		setSupportsOrderBy(true);
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(false);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(false);
		setSupportedJoinCriteria(SupportedJoinCriteria.KEY);
		setTransactionSupport(TransactionSupport.NONE);
		setSupportsDirectQueryProcedure(false);
	}

    @Override
    public void start() throws TranslatorException {
        super.start();
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            InfinispanConnection connection) throws TranslatorException {
        connection.registerProtobufFile(this.protobuf);
        return new InfinispanQueryExecution(this, command, executionContext, metadata, connection);
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            InfinispanConnection connection) throws TranslatorException {
        return new InfinispanUpdateExecution(command, executionContext, metadata,
                connection);
    }

    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata, InfinispanConnection connection)
            throws TranslatorException {
        return new InfinispanDirectExecution(arguments, command, executionContext, metadata, connection);
    }

    @Override
    public void getMetadata(MetadataFactory metadataFactory, InfinispanConnection conn) throws TranslatorException {
        ProtobufMetadataProcessor metadataProcessor = (ProtobufMetadataProcessor)getMetadataProcessor();
        PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getModelProperties(), "importer"); //$NON-NLS-1$
        metadataProcessor.process(metadataFactory, conn);
        this.protobuf = metadataProcessor.getProtobufResource();
    }

    @Override
    public MetadataProcessor<InfinispanConnection> getMetadataProcessor() {
        return new ProtobufMetadataProcessor();
    }

    @Override
    public int getMaxFromGroups() {
        return 1;
    }

    @Override
    public boolean isSourceRequiredForCapabilities() {
        return true;
    }

    @Override
    public boolean supportsAliasedTable() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsIsNullCriteria() {
        return false;
    }

    @Override
    public boolean supportsOrCriteria() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @TranslatorProperty(display="CompareCriteriaOrdered", description="If true, translator can support comparison criteria with the operator '=>' or '<=' ",advanced=true)
    @Override
    public boolean supportsCompareCriteriaOrdered() {
        return supportsCompareCriteriaOrdered;
    }

    public boolean setSupportsCompareCriteriaOrdered(boolean supports) {
        return supportsCompareCriteriaOrdered = supports;
    }

    @TranslatorProperty(display="Upsert", description="If true, translator can support Upsert command",advanced=true)
    @Override
    public boolean supportsUpsert() {
        return supportsUpsert;
    }

    public boolean setSupportsUpsert(boolean supports) {
        return supportsUpsert = supports;
    }

    @Override
    public boolean supportsLikeCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return true;
    }

    @Override
    public boolean supportsNotCriteria() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsRowOffset() {
        return true;
    }

    @Override
    public boolean useAnsiJoin() {
        return true;
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }
    @Override
    public boolean supportsAggregatesSum() {
        return false;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return true;
    }

    @Override
    public boolean supportsHaving() {
        return true;
    }
}
