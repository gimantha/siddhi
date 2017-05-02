package org.wso2.siddhi.extensions.table.solr.exceptions;

/**
 * This exception represents the Exception which might be thrown if there are any issues in the indexers
 */

public class SolrClientServiceException extends Exception {

    public SolrClientServiceException(String message) {
        super(message);
    }

    public SolrClientServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
