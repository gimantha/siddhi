package org.wso2.siddhi.extensions.table.solr;

import org.wso2.siddhi.extensions.table.solr.beans.SolrIndexDocument;
import org.wso2.siddhi.extensions.table.solr.beans.SolrSchema;
import org.wso2.siddhi.extensions.table.solr.config.CollectionConfiguration;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.table.solr.impl.SiddhiSolrClient;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrSchemaNotFoundException;

import java.util.List;

/**
 * This class represents the indexer interface. This can be used to implement the indexing implementation based on Solr
 */
public interface SolrClientService {
    /**
     * Returns the indexingClient for the specific tenant's table. Can be used to add, delete, update query/perform searches the tables' index
     * @return {@link org.wso2.siddhi.extensions.table.solr.impl.SiddhiSolrClient} A wrapper for {@link org.apache.solr.client.solrj.SolrClient}
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException Exception thrown if something goes wrong while creating or retrieving the client.
     */
    public SiddhiSolrClient getSolrServiceClient() throws SolrClientServiceException;

    /**
     * Create the Index/core/collection for the given table
     * @param configuration The collection configurations for which the index is created.
     * @return Returns true if successful, otherwise false
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException Exception thrown if something goes wrong while creating the index.
     */
    public boolean createCollection(CollectionConfiguration configuration)
            throws SolrClientServiceException;

    /**
     * Update or create the schema for the index of a specific table.
     * @param table Tablename of which the schema of the index being created
     * @param solrSchema The indexing Schema which represents the solr schema for the solr index/collection
     * @return returns true if successful, otherwise false
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException Exception thrown if something goes wrong while updating the index schema.
     */
    public boolean updateSolrSchema(String table, SolrSchema solrSchema, boolean merge) throws
                                                                                        SolrClientServiceException;

    /**
     * Returns the indexSchema of a table of a tenant domain
     * @param table Name of the table
     * @return {@link org.wso2.siddhi.extensions.table.solr.beans.SolrSchema}
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException Exception thrown if something goes wrong while retrieving the indexSchema
     */
    public SolrSchema getSolrSchema(String table)
            throws SolrClientServiceException, SolrSchemaNotFoundException;

    /**
     * Delete the index for a specific table in a tenant domain. The schema also will be deleted.
     * @param table Name of the table of which the index should be deleted
     * @return return true if successful, otherwise false
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException Exception thrown if something goes wrong while deleting the index.
     */
    public boolean deleteCollection(String table) throws SolrClientServiceException;

    /**
     * Checks if the index for a specific table exists or not.
     *
     * @param table Name of the table for the index being checked
     * @return True if there is an index for the given table, otherwise false
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException Exception is thrown if something goes wrong.
     */
    public boolean collectionExists(String table) throws SolrClientServiceException;

    /**
     * Checks if the index configuration exists or not.
     *
     * @param table The name of the table for the index being checked
     * @return True of the configurations exists otherwise false
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException Exceptions is thrown if something goes wrong.
     */
    public boolean collectionConfigExists(String table) throws SolrClientServiceException;

    /**
     * Inserts records as Solr documents to Solr index.
     * @param table The name of the table from which the documents/records are indexed
     * @param docs Documents which represents the records
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException Exceptions is thrown if something goes wrong.
     */
    public void insertDocuments(String table, List<SolrIndexDocument> docs) throws SolrClientServiceException;

    /**
     * Delete index documents by given document/record ids
     * @param table the name of the table to which the records belong
     * @param ids list of ids of records to be deleted
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException
     */
    public void deleteDocuments(String table, List<String> ids) throws SolrClientServiceException;

    /**
     * Delete index documents which match the given solr query
     * @param table the name of the table to which the records belong
     * @param query the solr query to filter out the records to be deleted
     * @throws org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException
     */
    public void deleteDocuments(String table, String query) throws SolrClientServiceException;

    /**
     * Closes the internally maintained Solr clients
     * @throws java.io.IOException
     */
    public void destroy() throws SolrClientServiceException;

}
