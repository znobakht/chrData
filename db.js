const mongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017';

const dbName1 = '20220628';
let client;
async function main(){
    try{
        client = await mongoClient.connect(url);
        console.log('connected to db');

        const db = client.db(dbName1);
        const collections = await db.listCollections().toArray();
        console.log(collections[0].name)
        // let collection = db.collection(collections[0].name);
        // let out = await collection.find({}).limit(10).toArray();
        // console.log(out)
        // collection = db.collection('cache_Init proc cause');
        // out = await collection.find({}).limit(10).toArray();
        // console.log(out)
        // for(i = 0 ; i < collections.length; i++){
        for(i = 0 ; i < 1; i++){
            console.log(i)
            // let collection = db.collection('chr');
            // let output = await db.collection(collections[i].name).find().limit(1).toArray()
            // console.log(output)
            // let output = await collection.aggregate([
            //     { $project: {  startTime: 1}},
            //     {$limit: 10}
            // ])
            // console.log(output)
            let collection = db.collection(collections[i].name);
            // let output = await db.collection(collections[i].name).find().limit(10)
            // console.log(output)
            let output = await collection.aggregate([
                { $project: { fieldName: `${collections[i].name}`, value: 1, name: 1, ts: 1, ProtocolCause: 1, 'Procedure identification': 1, _id: 0 }},
                {$limit: 2},
                {$out: "aggregation"}
            ]).toArray();
            // console.log(output)

            // output = await collection.find({}).limit(2).toArray()
            // console.log(output);
        }

    } catch (err) {
        console.error(err)
    }
    
}

main()
    .then(()=>{
        client.close();
    })
    .catch((err)=>{
        console.log(err);
        console.error(err);
        client.close();
    })