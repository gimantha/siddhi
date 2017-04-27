package org.wso2.siddhi.extensions.recordtable.solr.beans;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the fields in the solr schema
 */
public class SolrSchemaField implements Serializable {

    private static final long serialVersionUID = 7243610548183777241L;
    private Map<String, Object> properties;

    /**
     * Common attributes used to describe each field
     */
    public static final String ATTR_FIELD_NAME = "name";
    public static final String ATTR_INDEXED = "indexed";
    public static final String ATTR_STORED = "stored"; //Can be removed because of docValues
    public static final String ATTR_TYPE = "type";

    /**
     * Fields used internally
     */
    public static final String FIELD_TIMESTAMP = "_timestamp";
    public static final String FIELD_ID = "id";
    public static final String FIELD_VERSION = "_version_";

    public SolrSchemaField() {
        properties = new HashMap<>();
    }

    public SolrSchemaField(Map<String, Object> properties) {
        this.properties = new HashMap<>();
        if (properties != null) {
            this.properties.putAll(properties);
        }
    }


    public SolrSchemaField(SolrSchemaField solrSchemaField) {
        this();
        if (solrSchemaField != null) {
            this.properties = solrSchemaField.getProperties();
        }

    }

    public Map<String, Object> getProperties() {
        return new HashMap<>(properties);
    }

    public void setProperties(Map<String, Object> otherProperties) {
        if (otherProperties != null) {
            this.properties.putAll(otherProperties);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SolrSchemaField that = (SolrSchemaField) o;

        if (properties != null ? !properties.equals(that.properties) : that.properties != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return properties.hashCode();
    }
}
