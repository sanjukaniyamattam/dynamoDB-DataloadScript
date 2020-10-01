const fs    = require('fs-extra');
const parse = require('csv-parse');
const async = require('async');
const AWS   = require('aws-sdk');
const uuid   = require('uuid');
// import { v4 as uuidv4 } from 'uuid';

// console.log(uuidv4.v4());
// return;
// --- start user config ---

const AWS_CREDENTIALS_PROFILE = 'afod-aws-admin';
const CSV_FILENAME = "./file/Repairs/Ipad/repairs-Ipad-data-4.csv";
const DYNAMODB_REGION = 'ap-south-1';
const DYNAMODB_TABLENAME = 'services';

// --- end user config ---

const credentials = new AWS.SharedIniFileCredentials({
  profile: AWS_CREDENTIALS_PROFILE
});

AWS.config.credentials = credentials;
const docClient = new AWS.DynamoDB.DocumentClient({
  region: DYNAMODB_REGION
});

const rs = fs.createReadStream(CSV_FILENAME);
const parser = parse({
  columns: true,
  delimiter: ','
}, function(err, data) {

  var split_arrays = [],
    size = 25;

  while (data.length > 0) {
    split_arrays.push(data.splice(0, size));
  }

//   console.log(split_arrays);
//   return;
  data_imported = false;
  chunk_no = 1;

  async.each(split_arrays, function(item_data, callback) {
    const params = {
      RequestItems: {}
    };
    params.RequestItems[DYNAMODB_TABLENAME] = [];
    item_data.forEach(item => {
      for (key of Object.keys(item)) {
        // An AttributeValue may not contain an empty string
        if (item[key] === '')
          delete item[key];
      }
      item.id=uuid.v4();
      console.log(item);

      params.RequestItems[DYNAMODB_TABLENAME].push({
        PutRequest: {
          Item: {
            //id: Math.floor(Math.random() * 10),
            ...item
          }
        }
      });
    });

    docClient.batchWrite(params, function(err, res, cap) {
      console.log('done going next');
      if (err == null) {
        console.log('Success chunk #' + chunk_no);
        data_imported = true;
      } else {
        console.log(err);
        console.log('Fail chunk #' + chunk_no);
        data_imported = false;
      }
      chunk_no++;
      callback();
    });

  }, function() {
    // run after loops
    console.log('all data imported....');

  });

});
rs.pipe(parser);