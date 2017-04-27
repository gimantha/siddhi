package org.wso2.siddhi.extensions.recordtable.solr.beans;

import org.apache.solr.common.SolrInputDocument;
import org.wso2.siddhi.extensions.recordtable.solr.utils.IndexerUtils;

import java.util.Map;

/**
 * This represents a Solr document which is created from a DAL record.
 */
public class SolrIndexDocument extends SolrInputDocument {

    public SolrIndexDocument(String... fields) {
        super(fields);
    }

    public SolrIndexDocument(Map<String, SolrIndexDocumentField> fields) {
        super(IndexerUtils.getSolrFields(fields));
    }
}
