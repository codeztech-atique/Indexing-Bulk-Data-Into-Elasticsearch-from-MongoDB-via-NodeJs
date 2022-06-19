const elasticsearch = require('elasticsearch');
const config = require ('config');
const chalk = require('chalk');
const moment = require('moment');
const mongoClient = require('mongodb').MongoClient;
// const url = config.mongodb.url+config.mongodb.local_server_ip+":"+config.mongodb.port; // For local
const url = config.mongodb.url+""+config.mongodb.username+":"+config.mongodb.password+"@"+config.mongodb.serverUrl+"/"+config.mongodb.databaseName+'?retryWrites=true&w=majority';


const elasticClient = new elasticsearch.Client({
    host: config.elasticsearch.url + "" + config.elasticsearch.port,
    requestTimeout: 6 * 350 * 25000,
    requestTimeout: Infinity,
    keepAlive: false
    // log: 'debug',
})


var itemQue = [];
var limitData = 1000
var offset = 0;
var prev =0;
var iIndex =1;



//Function to bulk copy data to elastic search
function bulkop(data, callback) {
  elasticClient.bulk({
      body: data
  }, function(error, response) {
      if (callback)
      callback(error, response);
  });
  data = [];
};

//Function to format date
Date.prototype.yyyymmdd = function() {
  var yyyy = this.getFullYear().toString();
  var mm = (this.getMonth() + 1).toString();
  var dd = this.getDate().toString();

  return yyyy + '/' + (mm[1] ? mm : "0" + mm[0]) + '/' + (dd[1] ? dd : "0" + dd[0]);
};



function IndexMongodbData (esIndexName,esIndexType,collectionName)
{
    mongoClient.connect(url,{ useNewUrlParser:true, useUnifiedTopology: true },function(err, client) {
      if (err) {
          console.log(chalk.yellow('Sorry unable to connect to MongoDB Error:\n'), chalk.red(err));
      } else {

      // mongodb
      console.log(chalk.yellow("Connected successfully to server", url));
      var db = client.db(config.mongodb.databaseName);
      db.collection(collectionName, function (err, collection) {

      collection.find({}).skip(offset).limit(limitData).sort( { _id: -1 } ).toArray(function(err, result) {
        if (result.length>0)
        {
          process.nextTick(function(){
              result.forEach(element => {
                  for(prop in element)
                  {
                      if (typeof element[prop] === 'object')
                      {
                          if (prop.indexOf('_DATE')!= -1)
                          {
                              var m = moment(new Date(element[prop]).yyyymmdd(), ["MM/DD/YYYY", "YYYY/MM/DD", "DD/MM/YYYY"]);
                              if (m.isValid())
                              {                 
                                  element[prop] = m;
                              }
                              else
                                  delete element[prop];
                          }
                      }
                  }
                  if (element._id)
                  {
                      itemQue.push
                      ({
                          index: {
                              _index: esIndexName,
                              _type: esIndexType,
                              _id: element._id 
                          }
                      });
                      delete element._id;
                  }
                  else{
                      itemQue.push
                          ({
                              index: {
                                  _index: esIndexName,
                                  _type: esIndexType,
                                  _id: iIndex
                              }
                          });
                          // console.log(itemQue);
                  }
                  itemQue.push(JSON.stringify(element));
                  // console.log(itemQue);
                  iIndex++;
              });//End For loop
              
              if (itemQue.length >0)
              {
                  // console.log(JSON.stringify(itemQue));
                  bulkop(itemQue,function (err,res){
                      prev = offset;   
                      offset =offset + limitData ;
                  
                      console.log(chalk.blue("prevSet :" + prev + " newSet : " + offset))
                      if (err)
                          console.log(err)
                      else if(res)
                      {   
                          console.log(chalk.green("Data Items added succesfully :" + res.items.length  ))
                          IndexMongodbData(esIndexName,esIndexType,collectionName);
                      }
                  })//end Bulk copy Elastic search   
              }
              else
              {
                  process.exit()
              }
            });//Process Next tick
          }//end if result  
          else {
            console.log(chalk.red("All the data successfully imported into the Elasticsearch!"));
            process.exit()
          }
        });//end select query mongo collection table
      })
    }
  });
}


function DeleteMappings(esIndexName)
{
    elasticClient.indices.delete({
        index: esIndexName //delete all indices '_all'
    }, function(err, res) {
    
        if (err) {
            console.error(chalk.red(err.message));
        } else {
            console.log(chalk.yellow('Indices have been deleted!', esIndexName));
        }
    });   
}

module.exports = {IndexMongodbData, DeleteMappings};