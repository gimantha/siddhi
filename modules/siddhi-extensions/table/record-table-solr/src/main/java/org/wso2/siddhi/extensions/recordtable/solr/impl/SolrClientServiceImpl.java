package org.wso2.siddhi.extensions.recordtable.solr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrException;
import org.wso2.siddhi.extensions.recordtable.solr.beans.SolrIndexDocument;
import org.wso2.siddhi.extensions.recordtable.solr.config.CollectionConfiguration;
import org.wso2.siddhi.extensions.recordtable.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.recordtable.solr.utils.IndexerUtils;
import org.wso2.siddhi.extensions.recordtable.solr.SolrClientService;
import org.wso2.siddhi.extensions.recordtable.solr.beans.SolrSchema;
import org.wso2.siddhi.extensions.recordtable.solr.beans.SolrSchemaField;
import org.wso2.siddhi.extensions.recordtable.solr.exceptions.SolrSchemaNotFoundException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

/**
 * This class represents a concrete implementation of {@link org.wso2.siddhi.extensions.recordtable.solr.SolrClientService}
 */
public class SolrClientServiceImpl implements SolrClientService {

    private static final String ATTR_ERRORS = "errors";
    private static final String ATTR_COLLECTIONS = "collections";
    private static Log log = LogFactory.getLog(SolrClientServiceImpl.class);
    private volatile SiddhiSolrClient indexerClient = null;
    private static final String INDEXER_CONFIG_DIR = "analytics";
    private static final String INDEXER_CONFIG_FILE = "indexer-config.xml";
    private CollectionConfiguration glabalCollectionConfig;
    private final Object indexClientsLock = new Object();
    private Map<String, SolrSchema> solrSchemaCache = new ConcurrentHashMap<>();

    public SolrClientServiceImpl() {
        try {
            glabalCollectionConfig = loadGlobalCollectionConfigurations();
        } catch (SolrClientServiceException e) {
            log.error("Failed to initialize Indexer service : " + e.getMessage(), e);
        }
    }

    private CollectionConfiguration loadGlobalCollectionConfigurations() throws SolrClientServiceException {
        File confFile = new File(IndexerUtils.getIndexerConfDirectory() + File.separator +
                                 INDEXER_CONFIG_DIR + File.separator +
                                 INDEXER_CONFIG_FILE);
        try {
            if (!confFile.exists()) {
                confFile = IndexerUtils.getFileFromSystemResources(INDEXER_CONFIG_FILE);
                throw new SolrClientServiceException("the indexer service configuration file cannot be found at: " +
                                           confFile.getPath());
            }
            JAXBContext ctx = JAXBContext.newInstance(CollectionConfiguration.class);
            Unmarshaller unmarshaller = ctx.createUnmarshaller();
            return (CollectionConfiguration) unmarshaller.unmarshal(confFile);
        } catch (JAXBException e) {
            throw new SolrClientServiceException(
                    "Error in processing analytics indexer service configuration: " + e.getMessage(), e);
        } catch (URISyntaxException e) {
            throw new SolrClientServiceException("Error while loading configurations: cannot find the indexer configuration files in " +
                confFile.getPath() + " or from classpath.", e);
        }
    }

    @Override
    public SiddhiSolrClient getSolrServiceClient() throws SolrClientServiceException {
        if (indexerClient == null) {
            synchronized (indexClientsLock) {
                if (indexerClient == null) {
                    if (glabalCollectionConfig != null) {
                        SolrClient client = new CloudSolrClient.Builder().withZkHost(glabalCollectionConfig.getSolrServerUrl()).build();
                        indexerClient = new SiddhiSolrClient(client);
                    }
                }
            }
        }
        return indexerClient;
    }

    @Override
    public boolean createCollection(CollectionConfiguration config)
            throws SolrClientServiceException {
        String table = config.getCollectionName();
        String configSet = config.getConfigSet();
        if (configSet == null || configSet.trim().isEmpty()) {
            configSet = table;
        }
        String tableNameWithTenant = IndexerUtils.getCollectionNameWithDomainName(table);
        try {
            if (!indexExists(table)) {
                if (!indexConfigsExists(configSet)) {
                    ConfigSetAdminResponse configSetResponse = createInitialIndexConfiguration(config);
                    Object errors = configSetResponse.getErrorMessages();
                    if (configSetResponse.getStatus() == 0 && errors == null) {
                        return createSolrCollection(table, tableNameWithTenant);
                    } else {
                        throw new SolrClientServiceException("Error in deploying initial index configurations for table: " + table + ", " +
                                ", Response code: " + configSetResponse.getStatus() + " , errors: " + errors.toString());
                    }
                } else {
                    return createSolrCollection(table, tableNameWithTenant);
                }
            }
            return false;
        } catch (SolrServerException | IOException e) {
            log.error("error while creating the index for table: " + table + ": " + e.getMessage(), e);
            throw new SolrClientServiceException("error while creating the index for table: " + table + ": " + e.getMessage(), e);
        }
    }

    /*
    This method is to create the initial index configurations for the index of a table. This will include a default
    indexSchema and other Solr configurations. Later by using updateSolrSchema we can edit the index schema
    */
    private ConfigSetAdminResponse createInitialIndexConfiguration(CollectionConfiguration config)
            throws SolrServerException, IOException,
                   SolrClientServiceException {
        String tableNameWithTenant = IndexerUtils.getCollectionNameWithDomainName(config.getCollectionName());
        ConfigSetAdminRequest.Create configSetAdminRequest = new ConfigSetAdminRequest.Create();
        if (config.getConfigSet() != null && !config.getConfigSet().trim().isEmpty()) {
            configSetAdminRequest.setBaseConfigSetName(config.getConfigSet());
        } else {
            configSetAdminRequest.setBaseConfigSetName(glabalCollectionConfig.getConfigSet());
        }
        configSetAdminRequest.setConfigSetName(tableNameWithTenant);
        return configSetAdminRequest.process(getSolrServiceClient());
    }

    private boolean createSolrCollection(String table,  String tableNameWithTenant) throws SolrServerException, IOException,
                                                                                           SolrClientServiceException {
        CollectionAdminRequest.Create createRequest =
                CollectionAdminRequest.createCollection(tableNameWithTenant, tableNameWithTenant,
                                                        glabalCollectionConfig.getNoOfShards(), glabalCollectionConfig.getNoOfReplicas());
        createRequest.setMaxShardsPerNode(glabalCollectionConfig.getNoOfShards());
        CollectionAdminResponse collectionAdminResponse = createRequest.process(getSolrServiceClient());
        Object errors = collectionAdminResponse.getErrorMessages();
        if (!collectionAdminResponse.isSuccess()) {
            throw new SolrClientServiceException("Error in deploying initial index configurations for table: " + table + ", " +
                                       ", Response code: " + collectionAdminResponse.getStatus() + " , errors: " + errors.toString());
        }
        return true;
    }

    @Override
    public boolean updateSolrSchema(String table, SolrSchema solrSchema, boolean merge)
            throws SolrClientServiceException {
        SolrSchema oldSchema;
        List<SchemaRequest.Update> updateFields = new ArrayList<>();
        SolrClient client = getSolrServiceClient();
        String tableNameWithTenantDomain = IndexerUtils.getCollectionNameWithDomainName(table);
        SchemaResponse.UpdateResponse updateResponse;
        try {
            oldSchema = getIndexSchema(table);
        } catch (SolrSchemaNotFoundException e) {
            throw new SolrClientServiceException("Error while retrieving  the Solr schema for table: " + table, e);
        }
        updateFields = createUpdateFields(solrSchema, merge, oldSchema, updateFields);
        SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(updateFields);
        try {
            updateResponse = multiUpdateRequest.process(client, table);
            //UpdateResponse does not have a "getErrorMessages()" method, so we check if the errors attribute exists in the response
            Object errors = updateResponse.getResponse().get(ATTR_ERRORS);
            if (updateResponse.getStatus() == 0 && errors == null) {
                if (merge) {
                    SolrSchema mergedSchema = IndexerUtils.getMergedIndexSchema(oldSchema, solrSchema);
                    solrSchemaCache.put(tableNameWithTenantDomain, mergedSchema);
                } else {
                    solrSchemaCache.put(tableNameWithTenantDomain, solrSchema);
                }
                return true;
            } else {
                throw new SolrClientServiceException("Couldn't update index schema, Response code: " + updateResponse.getStatus() +
                        ", Errors: " + errors);
            }
        } catch (SolrServerException | IOException e) {
            log.error("error while updating the index schema for table: " + table + ": " + e.getMessage(), e);
            throw new SolrClientServiceException("error while updating the index schema for table: " + table + ": " + e.getMessage(), e);
        }
    }

    private List<SchemaRequest.Update> createUpdateFields(SolrSchema solrSchema, boolean merge,
                                                          SolrSchema finalOldSchema,
                                                          List<SchemaRequest.Update> updateFields) {
        //TODO: do we let users to change the uniqueKey "id"?
        if (!merge) {
            List<SchemaRequest.Update> oldFields = createSolrDeleteFields(finalOldSchema);
            List<SchemaRequest.Update> newFields = createSolrAddFields(solrSchema);
            updateFields.addAll(oldFields);
            updateFields.addAll(newFields);
        } else {
            updateFields = solrSchema.getFields().entrySet().stream()
                    .map(field -> finalOldSchema.getField(field.getKey()) != null ? updateSchemaAndGetReplaceFields(finalOldSchema, field) :
                     updateSchemaAndGetAddFields(finalOldSchema, field)).collect(Collectors.toList());
        }
        return updateFields;
    }

    private SchemaRequest.Update updateSchemaAndGetReplaceFields(SolrSchema oldSchema,
                                                                 Map.Entry<String, SolrSchemaField> field) {
        oldSchema.addField(field.getKey(), new SolrSchemaField(field.getValue()));
        return new SchemaRequest.ReplaceField(getSolrIndexProperties(field));
    }

    private SchemaRequest.Update updateSchemaAndGetAddFields(SolrSchema oldSchema,
                                                             Map.Entry<String, SolrSchemaField> field) {
        oldSchema.addField(field.getKey(), new SolrSchemaField(field.getValue()));
        return new SchemaRequest.AddField(getSolrIndexProperties(field));
    }

    private List<SchemaRequest.Update> createSolrAddFields(SolrSchema solrSchema) {
        List<SchemaRequest.Update> fields = new ArrayList<>();
        solrSchema.getFields().entrySet().stream().forEach(field -> {
            Map<String, Object> properties = getSolrIndexProperties(field);
            SchemaRequest.AddField addFieldRequest = new SchemaRequest.AddField(properties);
            fields.add(addFieldRequest);
        });
        return fields;
    }

    private Map<String, Object> getSolrIndexProperties(Map.Entry<String, SolrSchemaField> field) {
        Map<String, Object> properties = new HashMap<>();
        properties.putAll(field.getValue().getProperties());
        return properties;
    }

    private List<SchemaRequest.Update> createSolrDeleteFields(SolrSchema oldSchema) {
        List<SchemaRequest.Update> fields = new ArrayList<>();
        //TODO:add a config to define the default required field which should not be deleted. (e.g. id, there are other solr specific fields like _version_)
        oldSchema.getFields().entrySet().stream().filter(field -> !(field.getKey().equals(oldSchema.getUniqueKey()) ||
                field.getKey().equals(SolrSchemaField.FIELD_VERSION))).forEach(field -> {
            SchemaRequest.DeleteField deleteFieldRequest = new SchemaRequest.DeleteField(field.getKey());
            fields.add(deleteFieldRequest);
        });
        return fields;
    }

    @Override
    public SolrSchema getIndexSchema(String table)
            throws SolrClientServiceException, SolrSchemaNotFoundException {
        SolrClient client = getSolrServiceClient();
        String tableNameWithTenantDomain = IndexerUtils.getCollectionNameWithDomainName(table);
        SolrSchema solrSchema = solrSchemaCache.get(tableNameWithTenantDomain);
        if (solrSchema == null) {
            try {
                if (indexConfigsExists(table)) {
                    SchemaRequest.Fields fieldsRequest = new SchemaRequest.Fields();
                    SchemaRequest.UniqueKey uniqueKeyRequest = new SchemaRequest.UniqueKey();
                    SchemaResponse.FieldsResponse fieldsResponse = fieldsRequest.process(client, table);
                    SchemaResponse.UniqueKeyResponse uniqueKeyResponse = uniqueKeyRequest.process(client, table);
                    List<Map<String, Object>> fields = fieldsResponse.getFields();
                    String uniqueKey = uniqueKeyResponse.getUniqueKey();
                    solrSchema = createIndexSchemaFromSolrSchema(uniqueKey, fields);
                    solrSchemaCache.put(tableNameWithTenantDomain, solrSchema);
                } else {
                    throw new SolrSchemaNotFoundException("Index schema for table: " + table + "is not found");
                }
            } catch (SolrServerException | IOException | SolrException e) {
                log.error("error while retrieving the index schema for table: " + table + ": " + e.getMessage(), e);
                throw new SolrClientServiceException("error while retrieving the index schema for table: " + table + ": " + e.getMessage(), e);
            }
        }
        return solrSchema;
    }

    private static SolrSchema createIndexSchemaFromSolrSchema(String uniqueKey, List<Map<String, Object>> fields) throws
                                                                                                                  SolrClientServiceException {
        SolrSchema solrSchema = new SolrSchema();
        solrSchema.setUniqueKey(uniqueKey);
        solrSchema.setFields(createIndexFields(fields));
        return solrSchema;
    }

    private static Map<String, SolrSchemaField> createIndexFields(List<Map<String, Object>> fields) throws
                                                                                                    SolrClientServiceException {
        Map<String, SolrSchemaField> indexFields = new LinkedHashMap<>();
        String fieldName;
        for (Map<String, Object> fieldProperties : fields) {
            if (fieldProperties != null && fieldProperties.containsKey(SolrSchemaField.ATTR_FIELD_NAME)) {
                fieldName = fieldProperties.remove(SolrSchemaField.ATTR_FIELD_NAME).toString();
                indexFields.put(fieldName, new SolrSchemaField(fieldProperties));
            } else {
                throw new SolrClientServiceException("Fields must have an attribute called " + SolrSchemaField.ATTR_FIELD_NAME);
            }
        }
        return indexFields;
    }

    @Override
    public boolean deleteIndexForTable(String table) throws SolrClientServiceException {
        try {
            if (indexExists(table)) {
                String tableNameWithTenant = IndexerUtils.getCollectionNameWithDomainName(table);
                CollectionAdminRequest.Delete deleteRequest = CollectionAdminRequest.deleteCollection(tableNameWithTenant);
                CollectionAdminResponse deleteRequestResponse =
                        deleteRequest.process(getSolrServiceClient(), tableNameWithTenant);
                if (deleteRequestResponse.isSuccess() && indexConfigsExists(table)) {
                    ConfigSetAdminRequest.Delete configSetAdminRequest = new ConfigSetAdminRequest.Delete();
                    configSetAdminRequest.setConfigSetName(tableNameWithTenant);
                    ConfigSetAdminResponse configSetResponse = configSetAdminRequest.process(getSolrServiceClient());
                    solrSchemaCache.remove(tableNameWithTenant);
                    Object errors = configSetResponse.getErrorMessages();
                    if (configSetResponse.getStatus() == 0 && errors == null) {
                        return true;
                    } else {
                        throw new SolrClientServiceException("Error in deleting index for table: " + table + ", " +
                                                   ", Response code: " + configSetResponse.getStatus() + " , errors: " + errors.toString());
                    }
                }
            }
        } catch (IOException | SolrServerException e) {
            log.error("error while deleting the index for table: " + table + ": " + e.getMessage(), e);
            throw new SolrClientServiceException("error while deleting the index for table: " + table + ": " + e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean indexExists(String table) throws SolrClientServiceException {
        CollectionAdminRequest.List listRequest = CollectionAdminRequest.listCollections();
        String tableWithTenant = IndexerUtils.getCollectionNameWithDomainName(table);
        try {
            CollectionAdminResponse listResponse = listRequest.process(getSolrServiceClient());
            Object errors = listResponse.getErrorMessages();
            if (listResponse.getStatus() == 0 && errors == null) {
                List collections = (List) listResponse.getResponse().get(ATTR_COLLECTIONS);
                return collections.contains(tableWithTenant);
            } else {
                throw new SolrClientServiceException("Error in checking index for table: " + table + ", " +
                        ", Response code: " + listResponse.getStatus() + " , errors: " + errors.toString());
            }
        } catch (IOException | SolrServerException e) {
            log.error("Error while checking the existence of index for table : " + table, e);
            throw new SolrClientServiceException("Error while checking the existence of index for table : " + table, e);
        }
    }

    @Override
    public boolean indexConfigsExists(String table) throws SolrClientServiceException {
        ConfigSetAdminResponse.List listRequestReponse;
        SiddhiSolrClient siddhiSolrClient = getSolrServiceClient();
        String tableNameWithTenantDomain = IndexerUtils.getCollectionNameWithDomainName(table);
        ConfigSetAdminRequest.List listRequest = new ConfigSetAdminRequest.List();
        try {
            listRequestReponse = listRequest.process(siddhiSolrClient);
            Object errors = listRequestReponse.getErrorMessages();
            if (listRequestReponse.getStatus()== 0 && errors == null) {
                return listRequestReponse.getConfigSets().contains(tableNameWithTenantDomain);
            } else {
                throw new SolrClientServiceException("Error in checking the existance of index configuration for table: " + table + ", " +
                        ", Response code: " + listRequestReponse.getStatus() + " , errors: " + errors.toString());
            }
        } catch (IOException | SolrServerException e) {
            log.error("Error while checking if index configurations exists for table: " + table, e);
            throw new SolrClientServiceException("Error while checking if index configurations exists for table: " + table, e);
        }
    }

    @Override
    public void indexDocuments(String table, List<SolrIndexDocument> docs) throws SolrClientServiceException {
        try {
            SiddhiSolrClient client = getSolrServiceClient();
            client.add(table, IndexerUtils.getSolrInputDocuments(docs));
            //TODO:should find a better way to commit, there are different overloaded methods of commit
            client.commit(table);
        } catch (SolrServerException | IOException e) {
            log.error("Error while inserting the documents to index for table: " + table, e);
            throw new SolrClientServiceException("Error while inserting the documents to index for table: " + table, e);
        }
    }

    @Override
    public void deleteDocuments(String table, List<String> ids) throws SolrClientServiceException {
        if (ids != null && !ids.isEmpty()) {
            SiddhiSolrClient client = getSolrServiceClient();
            try {
                //TODO:use updateResponse
                client.deleteById(table, ids);
                client.commit(table);
            } catch (SolrServerException | IOException e) {
                log.error("Error while deleting index documents by ids, " + e.getMessage(), e);
                throw new SolrClientServiceException("Error while deleting index documents by ids, " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void deleteDocuments(String table, String query) throws SolrClientServiceException {
        if (query != null && !query.isEmpty()) {
            SiddhiSolrClient client = getSolrServiceClient();
            try {
                //TODO:use updateResponse
                client.deleteByQuery(table, query);
                client.commit(table);
            } catch (SolrServerException | IOException e) {
                log.error("Error while deleting index documents by query, " + e.getMessage(), e);
                throw new SolrClientServiceException("Error while deleting index documents by query, " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void destroy() throws SolrClientServiceException {
        try {
            if (indexerClient != null) {
                indexerClient.close();
            }
        } catch (IOException e) {
            log.error("Error while destroying the indexer service, " + e.getMessage(), e);
            throw new SolrClientServiceException("Error while destroying the indexer service, " + e.getMessage(), e);
        }
        indexerClient = null;
    }
}
