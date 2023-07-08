const AWS = require('aws-sdk');
AWS.config.update( {
  region: 'eu-north-1'
});
const dynamodb = new AWS.DynamoDB.DocumentClient();
const dynamodbTableName = 'streamers';
const streamerPath = '/streamer';
const streamersPath = '/streamers';
exports.handler = async function(event) {
  console.log('Request event: ', event);
  let response;
  switch(true) {
    case event.httpMethod === 'GET' && event.path === streamersPath:
      response = await getStreamers();
      break;
    case event.httpMethod === 'GET' && event.path.startsWith(streamerPath):
      const getStreamerID = event.pathParameters.streamerid;
      response = await getStreamer(getStreamerID);
      break;
    case event.httpMethod === 'POST' && event.path === streamerPath:
      response = await saveStreamer(JSON.parse(event.body));
      break;
    case event.httpMethod === 'PATCH' && event.path === streamerPath:
      const requestBody = JSON.parse(event.body);
      response = await modifyStreamer(requestBody.streamerID, requestBody.updateKey, requestBody.updateValue);
      break;
    case event.httpMethod === 'DELETE' && event.path.startsWith(streamerPath):
      const deleteStreamerID = event.pathParameters.streamerid;
      response = await deleteStreamer(deleteStreamerID);
      break;
    default:
      response = buildResponse(404, '404 Not Found');
  }
  return response;
}

async function getStreamer(streamerID) {
  const params = {
    TableName: dynamodbTableName,
    Key: {
      'streamerID': streamerID
    }
  };

  try {
    const response = await dynamodb.get(params).promise();
    if (!response.Item) {
      return buildResponse(400, 'There is no streamer with the specified ID');
    }
    return buildResponse(200, response.Item);
  } catch (error) {
    console.error('Error while getting streamer: ', error);
  }
}



async function getStreamers() {
  const params = {
    TableName: dynamodbTableName
  }
  const allStreamers = await scanDynamoRecords(params, []);
  const body = {
    streamers: allStreamers
  }
  return buildResponse(200, body);
}

async function scanDynamoRecords(scanParams, itemArray) {
  try {
    const dynamoData = await dynamodb.scan(scanParams).promise();
    itemArray = itemArray.concat(dynamoData.Items);
    if (dynamoData.LastEvaluatedKey) {
      scanParams.ExclusiveStartkey = dynamoData.LastEvaluatedKey;
      return await scanDynamoRecords(scanParams, itemArray);
    }
    return itemArray;
  } catch(error) {
    console.error('Error while checking streamers table ', error);
  }
}

async function saveStreamer(requestBody) {
    const { streamerID, platform, description } = requestBody;
  const existingStreamer = await getStreamer(streamerID);
    if (existingStreamer.statusCode === 200) {
    return buildResponse(400, 'Streamer already exists');
  }
  const params = {
    TableName: dynamodbTableName,
    Item: requestBody
  }
  return await dynamodb.put(params).promise().then(() => {
    const body = {
      Operation: 'SAVE',
      Message: 'SUCCESS',
      Item: requestBody
    }
    return buildResponse(200, body);
  }, (error) => {
    console.error('Error while saving streamer ', error);
  })
}

async function modifyStreamer(streamerID, updateKey, updateValue) {
  
    const existingStreamer = await getStreamer(streamerID);
    if (existingStreamer.statusCode !== 200) {
      return buildResponse(404, 'Streamer not found');
    }

    if (updateKey !== 'streamerID' && updateKey !== 'platform' && updateKey !== 'description') {
      return buildResponse(400, 'Invalid updateKey');
    }
  const params = {
    TableName: dynamodbTableName,
    Key: {
      'streamerID': streamerID
    },
    UpdateExpression: `set ${updateKey} = :value`,
    ExpressionAttributeValues: {
      ':value': updateValue
    },
    ReturnValues: 'UPDATED_NEW'
  }
  return await dynamodb.update(params).promise().then((response) => {
    const body = {
      Operation: 'UPDATE',
      Message: 'SUCCESS',
      UpdatedAttributes: response
    }
    return buildResponse(200, body);
  }, (error) => {
    console.error('Error while updating streamer: ', error);
  })
}

async function deleteStreamer(streamerID) {
     const existingStreamer = await getStreamer(streamerID);
    if (existingStreamer.statusCode !== 200) {
      return buildResponse(404, 'Streamer not found');
    }
  const params = {
    TableName: dynamodbTableName,
    Key: {
      'streamerID': streamerID
    },
    ReturnValues: 'ALL_OLD'
  }
  return await dynamodb.delete(params).promise().then((response) => {
    const body = {
      Operation: 'DELETE',
      Message: 'SUCCESS',
      Item: response
    }
    return buildResponse(200, body);
  }, (error) => {
    console.error('Error while deleting streamer  ', error);
  })
}

function buildResponse(statusCode, body) {
  return {
    statusCode: statusCode,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*', 
      'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, PUT', 
      'Access-Control-Allow-Headers': '*', 
    },
    body: JSON.stringify(body)
  }
}
