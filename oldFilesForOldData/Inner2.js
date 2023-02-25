import { MongoClient } from "mongodb";
import { mongoURL } from "./config/keys.js";

const dbName1 = "20220628";
// const dbName1 = 'tmpDB';
// const tmpDB = 'tmpDB';
// const finalDB = 'newDB1';
// const collectionName1 = 'newEx';
const collectionName1 = "cache_Inner cause";
let client;
async function main() {
  try {
    client = await MongoClient.connect(mongoURL);
    console.log("connected to db");

    const db = client.db(dbName1);
    const collection = db.collection(collectionName1);
    const unwantedNames = ["2", "3", "9", "ff"];
    const data = await collection.updateMany(
      { "Inner cause": { $in: unwantedNames } },
      { $set: { "Inner cause": "Unknown", name: "Unknown" } }
    );
    console.log(data);
    // await collection.updateMany({ _id: { $in: ids } }, { $set: { 'Inner cause': 'Unknown', name: 'Unknown' } })
  } catch (err) {
    console.error(err);
  }
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
