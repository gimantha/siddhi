package org.wso2.siddhi.extensions.recordtable.solr.exceptions;

/**
 * This exception is thrown when a given index schema is not found
 */
public class SolrSchemaNotFoundException extends Exception {

    public SolrSchemaNotFoundException(String message) {
        super(message);
    }

    public SolrSchemaNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
