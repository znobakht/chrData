const mongoClient = require('mongodb').MongoClient;
// const url = 'mongodb://localhost:27017';
const url = 'mongodb://root:Password!123@192.168.238.10:27017';

const dbName1 = '20220628';
const tmpDB = 'tmpDB';
const finalDB = 'newDB';
let client;
async function main() {
    try {
        client = await mongoClient.connect(url);
        console.log('connected to db');

        const db = client.db(dbName1);
        const collections = await db.listCollections().toArray();
        // console.log(collections);

        let time = new Date();
        console.log(time.getMinutes())

        for (i = 0; i < collections.length; i++) {
        // for (let i = 0; i < 2; i++) {
            let collection = db.collection(collections[i].name);
            await collection.aggregate([
                { $project: { fieldName: `${collections[i].name}`, value: 1, name: 1, ts: 1, ProtocolCause: 1, 'Procedure identification': 1, _id: 0 } },
                { $sort: { ProtocolCause: 1, 'Procedure identification': 1 }},
                { $out: { db: tmpDB, coll: "aggr" } }
            ]).toArray();
            let outputOfAggr = await client.db(tmpDB).collection("aggr").aggregate([
                { $group: { _id: { ProtocolCause: "$ProtocolCause", ProcedureIdentification: "$Procedure identification" } } },
            ]).toArray();

            const totalSize = await client.db(tmpDB).collection("aggr").count();
            console.log(totalSize);

            for (let j = 0; j < outputOfAggr.length; j++) {
                // for(i = 0; i<1; i++){
                let name = `${outputOfAggr[j]._id.ProcedureIdentification}_${outputOfAggr[j]._id.ProtocolCause}`;
                const outs = await client.db(tmpDB).collection("aggr").aggregate([
                    {
                        $match: {
                            ProtocolCause: `${outputOfAggr[j]._id.ProtocolCause}`, "Procedure identification": `${outputOfAggr[j]._id.ProcedureIdentification}`
                        }
                    },
                    { $merge: { into: { db: finalDB, coll: name }, whenMatched: "keepExisting" } }
                ]).toArray()

            }
        }

        time = new Date();
        console.log(time.getMinutes())
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