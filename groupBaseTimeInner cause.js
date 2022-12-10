const MongoClient = require('mongodb').MongoClient;
const url = "mongodb://root:Password!123@192.168.238.10:27017";

const dbName = "20220628";
const collectionName = "cache_Inner cause";
const tmpDB = 'tmpDB1';

let client;

async function main(){
    client = await MongoClient.connect(url);
    console.log('connected to db')
    const db = client.db(dbName);
    const colletion = db.collection(collectionName);
    await colletion.aggregate([
        {
            $match:{
            'Inner cause': 'Unknown'
        }
    },
        {
            $group:{
                _id: { 
                    ProtocolCause: "$ProtocolCause", 
                    ProcedureIdentification: "$Procedure identification", 
                    year: "$_id.year", month:"$_id.month", day:"$_id.day", hour:"$_id.hour" 
                },
                value: { $sum: "$value" },
                "Procedure identification": { "$first": "$Procedure identification" },
                "ProtocolCause": { "$first": "$ProtocolCause" },
                "ts": { "$first": "$ts" },
                "Inner cause": {"$first": "$Inner cause"}
            }
        },
        {
            $out: { db: tmpDB, coll: "cache_Inner cause"}
        }
    ]).toArray();
}


let time = new Date();
console.log(time.getMinutes())

main()
    .then(()=>{
        client.close();

        time = new Date();
        console.log(time.getMinutes())
    })
    .catch((err)=>{
        console.log(err);
        console.error(err);
        client.close()
    })