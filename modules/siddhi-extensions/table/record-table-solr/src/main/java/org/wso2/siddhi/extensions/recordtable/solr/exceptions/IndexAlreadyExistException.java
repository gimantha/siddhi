package org.wso2.siddhi.extensions.recordtable.solr.exceptions;

/**
 * This exception represents the error when user try to create the index for a table which already has an index with the same name
 */
public class IndexAlreadyExistException extends Exception {
    public IndexAlreadyExistException(String message) {
        super(message);
    }

    public IndexAlreadyExistException(String message, Throwable e) {
        super(message, e);
    }
}
