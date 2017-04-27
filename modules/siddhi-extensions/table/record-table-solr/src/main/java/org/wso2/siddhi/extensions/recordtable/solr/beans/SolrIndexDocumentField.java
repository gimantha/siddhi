package org.wso2.siddhi.extensions.recordtable.solr.beans;

import org.apache.solr.common.SolrInputField;

/**
 * This represents the IndexField which is input to solr index
 */
public class SolrIndexDocumentField extends SolrInputField {

    public SolrIndexDocumentField(String n) {
        super(n);
    }
}
