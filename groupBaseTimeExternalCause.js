import { MongoClient } from "mongodb";
import { mongoURL } from "./config/keys.js";

const dbName = "20220628";
const collectionName = "cache_ExternalCause";
const tmpDB = "tmpDB1";

let client;

async function main() {
  client = await MongoClient.connect(mongoURL);
  console.log("connected to db");
  const db = client.db(dbName);
  const colletion = db.collection(collectionName);
  const outs = await colletion
    .aggregate([
      {
        $match: {
          ExternalCause: "Unknown",
        },
      },
      {
        $group: {
          _id: {
            ProtocolCause: "$ProtocolCause",
            ProcedureIdentification: "$Procedure identification",
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            hour: "$_id.hour",
          },
          value: { $sum: "$value" },
          "Procedure identification": { $first: "$Procedure identification" },
          ProtocolCause: { $first: "$ProtocolCause" },
          ts: { $first: "$ts" },
          ExternalCause: { $first: "$ExternalCause" },
        },
      },
      // ,{
      //     $out: { db: tmpDB, coll: "cache_ExternalCause"}
      // }
    ])
    .toArray();

  await colletion.deleteMany({ ExternalCause: "Unknown" });
  await colletion.insertMany(outs);

  // console.log(outs);
}

let time = new Date();
console.log(time.getMinutes());

main()
  .then(() => {
    client.close();

    time = new Date();
    console.log(time.getMinutes());
  })
  .catch((err) => {
    console.log(err);
    console.error(err);
    client.close();
  });
