import { MongoClient } from "mongodb";
import { mongoURL } from "./config/keys.js";
const dbName = "20220628";
const collectionName = "cache_Inner cause";
const tmpDB = "tmpDB1";

let client;
async function main() {
  client = await MongoClient.connect(mongoURL);
  console.log(`Connected to db`);
  const db = client.db(dbName);
  const collection = db.collection(collectionName);
  await collection
    .aggregate([
      {
        $match: {
          "Inner cause": "Unknown",
        },
      },
      {
        $project: {
          year: { $year: "$ts" },
          month: { $month: "$ts" },
          day: { $dayOfMonth: "$ts" },
          hour: { $hour: "$ts" },
          "Inner cause": 1,
          value: 1,
          name: 1,
          ts: 1,
          ProtocolCause: 1,
          "Procedure identification": 1,
        },
      },
      {
        $out: {
          db: tmpDB,
          coll: "aggrTime",
        },
      },
    ])
    .toArray();
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
