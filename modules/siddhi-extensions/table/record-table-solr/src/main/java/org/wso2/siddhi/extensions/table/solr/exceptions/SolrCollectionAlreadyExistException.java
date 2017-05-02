package org.wso2.siddhi.extensions.table.solr.exceptions;

/**
 * This exception represents the error when user try to create the index for a table which already has an index with the same name
 */
public class SolrCollectionAlreadyExistException extends Exception {
    public SolrCollectionAlreadyExistException(String message) {
        super(message);
    }

    public SolrCollectionAlreadyExistException(String message, Throwable e) {
        super(message, e);
    }
}
