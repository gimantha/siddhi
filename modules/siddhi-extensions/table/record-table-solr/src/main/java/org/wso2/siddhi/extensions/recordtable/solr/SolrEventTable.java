/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extensions.recordtable.solr;

import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.table.record.AbstractRecordTable;
import org.wso2.siddhi.core.table.record.ConditionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.extensions.recordtable.solr.beans.SolrSchema;
import org.wso2.siddhi.extensions.recordtable.solr.config.CollectionConfiguration;
import org.wso2.siddhi.extensions.recordtable.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.recordtable.solr.impl.SolrClientServiceImpl;
import org.wso2.siddhi.extensions.recordtable.solr.utils.IndexerUtils;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.util.AnnotationHelper;


import java.util.List;
import java.util.Map;

/**
 * This class contains the Event table implementation for Solr which is running in the cloud mode.
 */

public class SolrEventTable extends AbstractRecordTable {

    private SolrClientService solrClientService;

    @Override
    protected void init(TableDefinition tableDefinition) {
        String primaryKey = null;
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                                                                         tableDefinition.getAnnotations());
        Annotation storeAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_STORE, tableDefinition
                .getAnnotations());

        if (primaryKeyAnnotation != null) {
            primaryKey = primaryKeyAnnotation.getElements().get(0).getValue().trim();
        }

        if (storeAnnotation != null) {
            String collectionName = storeAnnotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_COLLECTION);
            String url = storeAnnotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_URL);
            int shards = Integer.parseInt(storeAnnotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_SHARDS));
            int replicas = Integer.parseInt(storeAnnotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_REPLICA));
            String schema = storeAnnotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_SCHEMA);
            String configSet = storeAnnotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_CONFIGSET);

            if (collectionName == null || collectionName.trim().isEmpty()) {
                throw new ExecutionPlanCreationException("Solr collection name cannot be null or empty");
            }
            if (url == null || url.trim().isEmpty()) {
                throw new ExecutionPlanCreationException("SolrCloud url cannot be null or empty");
            }
            if (shards <= 0 || replicas <=0 ) {
                throw new ExecutionPlanCreationException("No of shards and no of replicas cannot be empty or less " +
                                                         "than" +
                                                         " 1");
            }
            SolrSchema solrSchema = IndexerUtils.createIndexSchema(primaryKey, schema);
            CollectionConfiguration collectionConfig = new CollectionConfiguration.Builder().collectionName
                    (collectionName).solrServerUrl(url).shards(shards).replicas(replicas).configs(configSet).schema
                    (solrSchema).build();
            //TODO: if schema is not given derive the schema from siddhi table schema
            solrClientService = new SolrClientServiceImpl();
            try {
                solrClientService.createCollection(collectionConfig);
                solrClientService.updateSolrSchema(collectionName, solrSchema, true);
            } catch (SolrClientServiceException e) {
                throw new ExecutionPlanCreationException("Error while initializing the Solr Event table: " + e
                        .getMessage(), e);
            }
        }
    }

    @Override
    protected void add(List<Object[]> records) {

    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap, CompiledCondition
            compiledCondition) {
        return null;
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap, CompiledCondition compiledCondition) {
        return false;
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition) {

    }

    @Override
    protected void update(List<Map<String, Object>> updateConditionParameterMaps, CompiledCondition compiledCondition,
                          List<Map<String, Object>> updateValues) {

    }

    @Override
    protected void updateOrAdd(List<Map<String, Object>> updateConditionParameterMaps,
                               CompiledCondition compiledCondition, List<Map<String, Object>> updateValues,
                               List<Object[]> addingRecords) {

    }

    @Override
    protected CompiledCondition compileCondition(ConditionBuilder conditionBuilder) {
        return null;
    }
}
