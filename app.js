const mongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017';

const dbName1 = '20220628';
let client;
async function main() {
    try {
        client = await mongoClient.connect(url);
        console.log('connected to db');

        const db = client.db(dbName1);
        const collections = await db.listCollections().toArray();

        const newDb = client.db("new1");


        // for (i = 0; i < collections.length; i++) {
        for (i = 0; i < 2; i++) {
            let collection = db.collection(collections[i].name);
            await collection.aggregate([
                { $project: { fieldName: `${collections[i].name}`, value: 1, name: 1, ts: 1, ProtocolCause: 1, 'Procedure identification': 1, _id: 0 } },
                { $sort: { ProtocolCause: 1, 'Procedure identification': 1 }},
                // { $group: { _id: { ProtocolCause: "$ProtocolCause", 'Procedure identification': "$Procedure identification" }, fieldName: { $first: '$fieldName' }, value: { $first: '$value' }, name: { $first: '$name' } }},
                // { $merge: { into: { db: "new", coll: "$fieldName"}} }
                // { $merge: "aggregation" }
                // { $out: "$fieldName" }
                { $out: { db: "new", coll: "aggr" } }
                // {$out: "aggr"}
                // { $out: { db: "new", coll: { $getField: "ProtocolCause" } }  }
                // { $out: { $getField: "ProtocolCause"}}
            ]).toArray();
            let output = await client.db("new").collection("aggr").aggregate([
                { $group: { _id: { ProtocolCause: "$ProtocolCause", ProcedureIdentification: "$Procedure identification" } } },
            ]).toArray();

            const totalSize = await client.db("new").collection("aggr").count();
            console.log(totalSize);

            // let j = 0;
            // let flag = 0;
            for (i = 0; i < output.length; i++) {
                // for(i = 0; i<1; i++){
                let name = `${output[i]._id.ProcedureIdentification}_${output[i]._id.ProtocolCause}`
                //    await newDb.createCollection(name);
                const outs = await client.db("new").collection("aggr").aggregate([
                    {
                        $match: {
                            ProtocolCause: `${output[i]._id.ProtocolCause}`, "Procedure identification": `${output[i]._id.ProcedureIdentification}`
                        }
                    },
                    { $merge: { into: { db: "new1", coll: name } } }
                ]).toArray()

            }
            console.log(i)
        }
        
        // console.log(output);
    } catch (err) {
        console.error(err)
    }

}

main()
    .then(() => {
        client.close();
    })
    .catch((err) => {
        console.log(err);
        console.error(err);
        client.close();
    })