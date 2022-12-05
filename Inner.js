const mongoClient = require('mongodb').MongoClient;
// const url = 'mongodb://localhost:27017';
const url = 'mongodb://root:Password!123@192.168.238.10:27017';

const dbName1 = '20220628';
// const dbName1 = 'tmpDB';
// const tmpDB = 'tmpDB';
// const finalDB = 'newDB1';
// const collectionName1 = 'newEx';
const collectionName1 = 'cache_Inner cause';
let client;
async function main() {
    try {
        client = await mongoClient.connect(url);
        console.log('connected to db');

        const db = client.db(dbName1);
        const collection = db.collection(collectionName1)
        const data = await collection.find({}, {projection:{ _id: 1, 'Inner cause':1, name:1 }}).toArray();
        const ids = [];
        let regex = /[0-9a-fA-F]{6}/;
        for(i=0; i<data.length; i++){
            if (data[i]["Inner cause"].match(regex)){
                // console.log(data[i]["Inner cause"])
                ids.push(data[i]._id);
            }
        }
        console.log(ids.length)
        await collection.updateMany({ _id: { $in: ids } }, { $set: { 'Inner cause': 'Unknown', name: 'Unknown' } })


    } catch (err) {
        console.error(err)
    }

}

let time = new Date();
console.log(time.getMinutes())

main()
    .then(() => {
        client.close();

        time = new Date();
        console.log(time.getMinutes())
    })
    .catch((err) => {
        console.log(err);
        console.error(err);
        client.close();

    })
