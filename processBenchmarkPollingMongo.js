const { MongoClient, ObjectId } = require("mongodb");
let cachedDb = null;


function sendResponse(callback, status, success, reponseData) {
    var apiResponse = {
        "statusCode": status,
        "headers": {
            'Access-Control-Allow-Origin': '*',
        },
        "body": JSON.stringify({ success: success, data: reponseData }),
        "isBase64Encoded": false
    };
    console.log(apiResponse);
    callback(null, apiResponse);
}


exports.handler = (event, context, callback) => {
    try {
        const body = JSON.parse(event.body);
        console.log(body, body.query, body.type);
        // const maxRetry = process.env.MAX_RETRY_BEFORE_EMAIL != undefined ? process.env.MAX_RETRY_BEFORE_EMAIL : 5;
        if (body.type == undefined || body.type == '') {
            sendResponse(callback, 500, false, "Please provide the type");
        } else {
            const db_name = process.env.MONGODB_DATABASE_NAME;
            var db_collection = process.env.MONGODB_COLLECTION_NAME;
            const uri = process.env.MONGODB_URI;
            if (body.collectionName) {
                db_collection = body.collectionName;
            }
            // context.callbackWaitsForEmptyEventLoop = false;
            console.log("message before try");
            try {
                console.log("message in try");
                MongoClient.connect(uri, { useUnifiedTopology: true }, function (err, client) {
                    console.log("message before if");
                    if (err) {
                        console.log("message if err");
                        console.log(err);
                        // sendResponse(callback, 500, false, "not able to connect to DB");
                        throw err;
                    }
                    else {
                        // if (cachedDb) {
                        //     // pass
                        // } else {
                        cachedDb = client.db(db_name);
                        // }
                        if (body.query && body.query._id) {
                            body.query._id = ObjectId(body.query._id);
                        }
                        if (body.queryTo && body.queryTo._id) {
                            body.queryTo._id = ObjectId(body.queryTo._id);
                        }
                        if (body.type == 'listCollections') {
                            cachedDb.listCollections().toArray(function (err, res) {
                                // collInfos is an array of collection info objects that look like:
                                if (err) throw err;
                                client.close();
                                console.log(res);
                                sendResponse(callback, 200, true, res);
                            });
                        }
                        else if (body.type == 'createCollection') {
                            cachedDb.createCollection(body.collectionName, function (err, res) {
                                if (err) throw err;
                                console.log("Collection created!");
                                client.close();
                                sendResponse(callback, 200, true, { "name": body.collectionName, "action": "Collection created!" });
                            });
                        }
                        else if (body.type == 'insertOne') {
                            if (body.data) {
                                cachedDb.collection(db_collection).insertOne(body.data, function (err, res) {
                                    if (err) throw err;
                                    console.log("1 document inserted", res);
                                    client.close();
                                    sendResponse(callback, 200, true, res);
                                });
                            } else {
                                sendResponse(callback, 500, false, "Please provide the data");
                            }
                        } else if (body.type == 'find') {
                            if (body.query) {
                                cachedDb.collection(db_collection).find(body.query).toArray(function (err, res) {
                                    if (err) throw err;
                                    console.log(res);
                                    client.close();
                                    sendResponse(callback, 200, true, res);
                                });
                            } else {
                                sendResponse(callback, 500, false, "Please provide the query");
                            }
                        } else if (body.type == 'updateOne') {
                            if (body.query) {
                                console.log(body.query);
                                cachedDb.collection(db_collection).updateOne(body.query, { $set: body.data }, function (err, res) {
                                    if (err) throw err;
                                    console.log(res);
                                    client.close();
                                    sendResponse(callback, 200, true, res);
                                });
                            } else {
                                sendResponse(callback, 500, false, "Please provide the query");
                            }
                        } else if (body.type == 'insertFromOne') {
                            cachedDb.collection(db_collection).find(body.query).toArray(function (err, res) {
                                if (err) throw err;
                                console.log(res);
                                var new_data = {}
                                new_data[body.attribute] = res[0][body.attribute]
                                console.log(new_data);
                                cachedDb.collection(body.collectionNameTo).updateOne(body.queryTo, { $set: new_data }, function (err, res) {
                                    if (err) throw err;
                                    console.log(res);
                                    client.close();
                                    sendResponse(callback, 200, true, res);
                                });
                            });
                        } else {
                            console.log({ success: false, data: "Please provide a correct workflow" });
                            sendResponse(callback, 500, false, "Please provide a correct type");
                        }
                    }
                });
            }
            catch (e) {
                console.log('Error updating mongo with body ' + body + ' with error ', e.stack);
                throw e;
            }
        }
    } catch (e) {
        console.log({ error: e, data: event.body });
        throw e;
    }
};
