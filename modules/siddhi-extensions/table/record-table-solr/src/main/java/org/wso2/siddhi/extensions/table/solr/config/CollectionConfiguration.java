package org.wso2.siddhi.extensions.table.solr.config;

import org.wso2.siddhi.extensions.table.solr.beans.SolrSchema;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Represents the Indexing Server details to connect.
 */
@XmlRootElement(name = "indexer-config")
public class CollectionConfiguration {
    private final static String DEFAULT_SOLR_URL = "localhost:9983";
    private final static String BASE_CONFIG_SET = "gettingstarted";
    private final static String DEFAULT_NO_OF_SHARDS = "2";
    private final static String DEFAULT_NO_OF_REPLICA = "1";
    private String solrServerUrl;
    private String collectionName;
    private int noOfShards;
    private int noOfReplicas;
    private String configSet;
    private SolrSchema schema;

    private CollectionConfiguration() {

    }

    private CollectionConfiguration(String collectionName, String solrServerUrl, int noOfShards, int noOfReplicas,
                                   SolrSchema schema, String configSet) {
        this.noOfShards = noOfShards;
        this.noOfReplicas = noOfReplicas;
        this.solrServerUrl = solrServerUrl;
        this.configSet = configSet;
        this.collectionName = collectionName;
        this.schema = schema;
    }

    @XmlElement(name = "solr-cloud-url", defaultValue = DEFAULT_SOLR_URL )
    public String getSolrServerUrl() {
        return solrServerUrl;
    }

    private void setSolrServerUrl(String solrServerUrl) {
        this.solrServerUrl = solrServerUrl;
    }

    @XmlElement(name = "no-of-shards", defaultValue = DEFAULT_NO_OF_SHARDS)
    public int getNoOfShards() {
        return noOfShards;
    }

    private void setNoOfShards(int noOfShards) {
        this.noOfShards = noOfShards;
    }

    @XmlElement(name = "no-of-replica", defaultValue = DEFAULT_NO_OF_REPLICA)
    public int getNoOfReplicas() {
        return noOfReplicas;
    }

    private void setNoOfReplicas(int noOfReplicas) {
        this.noOfReplicas = noOfReplicas;
    }

    @XmlElement(name = "default-config-set", defaultValue = BASE_CONFIG_SET)
    public String getConfigSet() {
        return configSet;
    }

    private void setConfigSet(String configSet) {
        this.configSet = configSet;
    }

    public String getCollectionName() {
        return collectionName;
    }

    private void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public SolrSchema getSchema() {
        return schema;
    }

    private void setSchema(SolrSchema schema) {
        this.schema = schema;
    }

    public static class Builder {
        private String solrServerUrl;
        private String collectionName;
        private int noOfShards;
        private int noOfReplicas;
        private String configSet;
        private SolrSchema schema;

        public Builder() {

        }

        public Builder solrServerUrl(String solrServerUrl) {
            this.solrServerUrl = solrServerUrl;
            return this;
        }

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder shards(int noOfShards) {
            this.noOfShards = noOfShards;
            return this;
        }

        public Builder replicas(int noOfReplicas) {
            this.noOfReplicas = noOfReplicas;
            return this;
        }

        public Builder configs(String configSet) {
            this.configSet = configSet;
            return this;
        }

        public Builder schema(SolrSchema schema) {
            this.schema = schema;
            return this;
        }

        public CollectionConfiguration build() {
            return new CollectionConfiguration(collectionName, solrServerUrl, noOfShards, noOfReplicas, schema,
                                               configSet);
        }
    }
}
