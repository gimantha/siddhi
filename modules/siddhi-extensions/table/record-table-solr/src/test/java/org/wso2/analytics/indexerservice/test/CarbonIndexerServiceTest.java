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

package org.wso2.analytics.indexerservice.test;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.wso2.siddhi.extensions.recordtable.solr.SolrClientService;
import org.wso2.siddhi.extensions.recordtable.solr.beans.SolrIndexDocument;
import org.wso2.siddhi.extensions.recordtable.solr.beans.SolrSchema;
import org.wso2.siddhi.extensions.recordtable.solr.beans.SolrSchemaField;
import org.wso2.siddhi.extensions.recordtable.solr.config.CollectionConfiguration;
import org.wso2.siddhi.extensions.recordtable.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.recordtable.solr.exceptions.SolrSchemaNotFoundException;
import org.wso2.siddhi.extensions.recordtable.solr.impl.SiddhiSolrClient;
import org.wso2.siddhi.extensions.recordtable.solr.utils.IndexerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * This class contains the unit tests for indexer service;
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CarbonIndexerServiceTest {

    private SolrClientService indexerService;


    @BeforeClass
    public void init() throws SolrClientServiceException {
        System.setProperty(IndexerUtils.WSO2_ANALYTICS_INDEX_CONF_DIRECTORY_SYS_PROP, "src/test/resources/conf");
        ServiceLoader<SolrClientService> analyticsIndexServiceServiceLoader = ServiceLoader.load(SolrClientService.class);
        if (this.indexerService == null) {
            this.indexerService = analyticsIndexServiceServiceLoader.iterator().next();
            if (this.indexerService == null) {
                throw new SolrClientServiceException("Indexer Service cannot be loaded!");
            }
        }
    }
    @Test
    public void step1_testCreateIndexForTable() throws SolrClientServiceException {
        CollectionConfiguration config = new CollectionConfiguration.Builder().collectionName("T1").build();
        indexerService.createCollection(config);
        Assert.assertTrue(indexerService.indexExists("T1"));
    }

    @Test
    public void step2_testCreateExistingIndex() throws SolrClientServiceException {
        CollectionConfiguration config = new CollectionConfiguration.Builder().collectionName("T1").build();
        Assert.assertTrue(indexerService.indexExists("T1"));
        Assert.assertFalse(indexerService.createCollection(config));
    }

    @Test
    public void step3_testIfInitialConfigsAreCopied() throws SolrClientServiceException {
        Assert.assertTrue(indexerService.indexConfigsExists("T1"));
    }

    @Test
    public void step4_testNonExistingIndexSchema() throws SolrClientServiceException {
        try {
            indexerService.getIndexSchema("T2");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SolrSchemaNotFoundException);
        }
    }

    @Test
    public void step5_testInitialIndexSchema() throws SolrClientServiceException, SolrSchemaNotFoundException {
        SolrSchema indexSchema = indexerService.getIndexSchema("T1");
        Assert.assertEquals(indexSchema.getUniqueKey(), "id");
    }

    @Test
    public void step6_testUpdateIndexSchemaWithoutMerge()
            throws SolrSchemaNotFoundException, SolrClientServiceException {
        Map<String, SolrSchemaField> fieldMap = new HashMap<>();
        SolrSchemaField intField = new SolrSchemaField();
        intField.setProperty("name", "IntField");
        intField.setProperty("indexed", true);
        intField.setProperty("type", "int");
        intField.setProperty("stored", true);
        SolrSchemaField longField = new SolrSchemaField();
        longField.setProperty("name", "LongField");
        longField.setProperty("indexed", true);
        longField.setProperty("type", "long");
        longField.setProperty("stored", true);
        SolrSchemaField floatField = new SolrSchemaField();
        floatField.setProperty("name", "FloatField");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "float");
        floatField.setProperty("stored", true);
        SolrSchemaField doubleField = new SolrSchemaField();
        floatField.setProperty("name", "DoubleField");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "double");
        floatField.setProperty("stored", true);
        SolrSchemaField boolField = new SolrSchemaField();
        floatField.setProperty("name", "BoolField");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "boolean");
        floatField.setProperty("stored", true);
        SolrSchemaField timestamp = new SolrSchemaField();
        floatField.setProperty("name", "_timestamp");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "long");
        floatField.setProperty("stored", true);
        fieldMap.put(intField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), intField);
        fieldMap.put(longField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), longField);
        fieldMap.put(doubleField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), doubleField);
        fieldMap.put(floatField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), floatField);
        fieldMap.put(boolField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), boolField);
        fieldMap.put(timestamp.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), timestamp);
        SolrSchema indexSchema = new SolrSchema("id", fieldMap);
        Assert.assertTrue(indexerService.updateSolrSchema("T1", indexSchema, false));
        SolrSchema indexSchema1 = indexerService.getIndexSchema("T1");
        Assert.assertEquals(indexSchema1.getField("IntField"), indexSchema.getField("IntField"));
        Assert.assertEquals(indexSchema1.getField("LongField"), indexSchema.getField("LongField"));
        Assert.assertEquals(indexSchema1.getField("DoubleField"), indexSchema.getField("DoubleField"));
        Assert.assertEquals(indexSchema1.getField("FloatField"), indexSchema.getField("FloatField"));
        Assert.assertEquals(indexSchema1.getField("BoolField"), indexSchema.getField("BoolField"));
        Assert.assertEquals(indexSchema1.getField("_timestamp"), indexSchema.getField("_timestamp"));
    }

    @Test
    public void step7_testUpdateIndexSchemaWithMerge()
            throws SolrSchemaNotFoundException, SolrClientServiceException {
        SolrSchema oldIndexSchema = indexerService.getIndexSchema("T1");
        Map<String, SolrSchemaField> fieldMap = new HashMap<>();
        SolrSchemaField intField = new SolrSchemaField();
        intField.setProperty("name", "IntField");
        intField.setProperty("indexed", true);
        intField.setProperty("type", "int");
        intField.setProperty("stored", true);
        SolrSchemaField longField = new SolrSchemaField();
        longField.setProperty("name", "LongField");
        longField.setProperty("indexed", true);
        longField.setProperty("type", "long");
        longField.setProperty("stored", true);
        SolrSchemaField floatField = new SolrSchemaField();
        floatField.setProperty("name", "FloatField");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "float");
        floatField.setProperty("stored", true);
        SolrSchemaField doubleField = new SolrSchemaField();
        floatField.setProperty("name", "DoubleField");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "double");
        floatField.setProperty("stored", true);
        SolrSchemaField boolField = new SolrSchemaField();
        floatField.setProperty("name", "BoolField");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "boolean");
        floatField.setProperty("stored", true);
        SolrSchemaField timestamp = new SolrSchemaField();
        floatField.setProperty("name", "_timestamp");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "long");
        floatField.setProperty("stored", true);
        fieldMap.put(intField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), intField);
        fieldMap.put(longField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), longField);
        fieldMap.put(doubleField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), doubleField);
        fieldMap.put(floatField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), floatField);
        fieldMap.put(boolField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), boolField);
        fieldMap.put(timestamp.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), timestamp);
        SolrSchema indexSchema = new SolrSchema("id", fieldMap);
        Assert.assertTrue(indexerService.updateSolrSchema("T1", indexSchema, true));
        SolrSchema newIndexSchema = indexerService.getIndexSchema("T1");
        Assert.assertEquals(newIndexSchema.getField("IntField1"), indexSchema.getField("IntField1"));
        Assert.assertEquals(newIndexSchema.getField("LongField1"), indexSchema.getField("LongField1"));
        Assert.assertEquals(newIndexSchema.getField("DoubleField1"), indexSchema.getField("DoubleField1"));
        Assert.assertEquals(newIndexSchema.getField("FloatField1"), indexSchema.getField("FloatField1"));
        Assert.assertEquals(newIndexSchema.getField("BoolField1"), indexSchema.getField("BoolField1"));

        Assert.assertEquals(newIndexSchema.getField("IntField"), oldIndexSchema.getField("IntField"));
        Assert.assertEquals(newIndexSchema.getField("LongField"), oldIndexSchema.getField("LongField"));
        Assert.assertEquals(newIndexSchema.getField("DoubleField"), oldIndexSchema.getField("DoubleField"));
        Assert.assertEquals(newIndexSchema.getField("FloatField"), oldIndexSchema.getField("FloatField"));
        Assert.assertEquals(newIndexSchema.getField("BoolField"), oldIndexSchema.getField("BoolField"));
        Assert.assertEquals(newIndexSchema.getField("_timestamp"), oldIndexSchema.getField("_timestamp"));
    }

    @Test
    public void step8_testIndexDocuments() throws SolrClientServiceException, IOException, SolrServerException {
        SolrIndexDocument doc1 = new SolrIndexDocument();
        doc1.addField("id", "1");
        doc1.addField("_timestamp", System.currentTimeMillis());
        doc1.addField("IntField", 100);
        doc1.addField("LongField", 100l);
        doc1.addField("FloatField", 100f);
        doc1.addField("DoubleField", 100d);
        doc1.addField("BoolField", true);
        SolrIndexDocument doc2 = new SolrIndexDocument();
        doc2.addField("id", "2");
        doc2.addField("IntField1", 1000);
        doc2.addField("LongField1", 1000l);
        doc2.addField("FloatField1", 1000f);
        doc2.addField("DoubleField1", 1000d);
        doc2.addField("BoolField1", true);
        List<SolrIndexDocument> docs = new ArrayList<>();
        docs.add(doc1);
        docs.add(doc2);
        indexerService.indexDocuments("T1", docs);

        SiddhiSolrClient client = indexerService.getSolrServiceClient();
        SolrQuery query = new SolrQuery();
        query.setQuery("id:1");

        QueryResponse response = client.query("T1", query);
        SolrDocumentList list = response.getResults();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(doc1.getFieldValue("id"), list.get(0).getFieldValue("id"));
        Assert.assertEquals(doc1.getFieldValue("DoubleField"), list.get(0).getFieldValue("DoubleField"));

        query = new SolrQuery();
        query.setQuery("id:2");
        QueryResponse response1 = client.query("T1", query);
        SolrDocumentList list1 = response1.getResults();
        Assert.assertEquals(list1.size(), 1);
        Assert.assertEquals(doc2.getFieldValue("id"), list1.get(0).getFieldValue("id"));
    }

    @Test
    public void step9_testDeleteDocumentsByTimeRange()
            throws SolrClientServiceException, IOException, SolrServerException {
        String strQuery = "_timestamp:[0 TO " + System.currentTimeMillis() + "]";
        indexerService.deleteDocuments("T1", strQuery);
        SiddhiSolrClient client = indexerService.getSolrServiceClient();
        SolrQuery query = new SolrQuery();
        query.setQuery(strQuery);
        QueryResponse response = client.query("T1", query);
        SolrDocumentList list = response.getResults();
        Assert.assertTrue(list.isEmpty());
    }

    @Test
    public void step10_testDeleteDocumentsByIds() throws SolrClientServiceException, IOException,
                                                  SolrServerException {
        List<String> ids = new ArrayList<>();
        ids.add("2");
        indexerService.deleteDocuments("T1",ids);
        SiddhiSolrClient client = indexerService.getSolrServiceClient();
        SolrQuery query = new SolrQuery();
        query.setQuery("id:2");
        QueryResponse response = client.query("T1", query);
        SolrDocumentList list = response.getResults();
        Assert.assertTrue(list.isEmpty());
    }

    @Test
    public void step11_testDeleteIndexForTable() throws SolrClientServiceException {
        Assert.assertTrue(indexerService.deleteIndexForTable("T1"));
    }

    @Test
    public void step12_testDeleteNonExistingIndex() throws SolrClientServiceException {
        Assert.assertFalse(indexerService.deleteIndexForTable("T1"));
    }

    @Test
    public void step13_testDestroy() throws SolrClientServiceException {
        indexerService.destroy();
    }
}
