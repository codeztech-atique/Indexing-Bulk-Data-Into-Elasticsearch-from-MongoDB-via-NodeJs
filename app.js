var indexer = require('./indexer.js');

var config = require ('config');

var indexname = config.elasticsearch.elasticsearchIndices.COMPANY.index;
var indextype = config.elasticsearch.elasticsearchIndices.COMPANY.type;
var tableName = config.elasticsearch.elasticsearchIndices.COMPANY.collectionName;

// indexer.IndexMongodbData(indexname,indextype,tableName); //Inserting bulk data into Elasticsearch, Kibana from mongodb
indexer.DeleteMappings(indexname);  //Deleting bulk data from Elasticsearch, Kibana from mongodb

