const mongoClient = require("mongodb").MongoClient;
const dbUrl = "mongodb://root:Password!123@192.168.238.10:27017";
const dbNameTemplate = "2022120";


let client;
async function main(){
    try{
        client = await mongoClient.connect(dbUrl);
        for(let i =5; i<8; i++){
        // for(let i =7; i<8; i++){
            console.log(`${i}started`)
            const db = client.db(`${dbNameTemplate}${i}`);
            const collection = db.collection("chr");
            await collection.updateMany({"ProtocolCause": {$in:['0', '65535']}},{$set:{"ProtocolCause":"Unknown"}});
            await collection.updateMany({"ProtocolCause": '17'}, {$set:{"ProtocolCause":"Network failure"}});
            await collection.updateMany({"ProtocolCause": '26'}, {$set:{"ProtocolCause":"Non-EPS authentication unacceptable"}});
            await collection.updateMany({"ProtocolCause": '33'}, {$set:{"ProtocolCause":"Requested service option not subscribed"}});
            await collection.updateMany({"ProtocolCause": '39'}, {$set:{"ProtocolCause":"Reactivation requested"}});
            await collection.updateMany({"ProtocolCause": '49'}, {$set:{"ProtocolCause":"Last PDN disconnection not allowed"}});
            await collection.updateMany({"ProtocolCause": '55'}, {$set:{"ProtocolCause":"Multiple PDN connections for a given APN not allowed"}});
            await collection.updateMany({"ProtocolCause": '96'}, {$set:{"ProtocolCause":"Invalid mandatory information"}});
            await collection.updateMany({"ProtocolCause": '99'}, {$set:{"ProtocolCause":"Information element non-existent or not implemented"}});
            await collection.updateMany({"ProtocolCause": '28'}, {$set:{"ProtocolCause":"Unknown PDN type"}});
            await collection.updateMany({"ProtocolCause": '41'}, {$set:{"ProtocolCause":"Semantic error in the TFT operation"}});
            await collection.updateMany({"ProtocolCause": '8'}, {$set:{"ProtocolCause":"EPS services and non-EPS services not allowed"}});
            await collection.updateMany({"ProtocolCause": 'Activation rejected by GGSNÂÂ¬ serving GW or PDN GW'}, {$set:{"ProtocolCause":"Activation rejected by GGSN, Serving GW or PDN GW"}});
            console.log(`${i}ended`)

        }


    }
    catch(err){
        console.error(err)
        client.close()
    }
}
console.log(new Date().getMinutes())
main()
    .then(()=>console.log(new Date().getMinutes()))
    .catch(err=>{
        console.log(new Date().getMinutes())
        console.error(err)
    })