import { MongoClient } from "mongodb";
import { mongoURL } from "./config/keys.js";

const dbName1 = "20220628";
// const dbName1 = 'tmpDB';
// const tmpDB = 'tmpDB';
// const finalDB = 'newDB1';
// const collectionName1 = 'newEx';
const collectionName1 = "cache_ExternalCause";
let client;
async function main() {
  try {
    client = await MongoClient.connect(mongoURL);
    console.log("connected to db");

    const db = client.db(dbName1);
    const collection = db.collection(collectionName1);
    const data = await collection
      .find({}, { projection: { _id: 1, ExternalCause: 1, name: 1 } })
      .toArray();
    /** if field of 'ExternalCause' doesnt be in the projection fields, update wont take place */
    const ids = [];
    for (i = 0; i < data.length; i++) {
      if (!isNaN(parseInt(data[i].ExternalCause))) {
        console.log(data[i].ExternalCause);
        ids.push(data[i]._id);
        // await collection.updateOne({ _id: data[i]._id }, { ExternalCause:'unknown'});
      }
    }
    await collection.updateMany(
      { _id: { $in: ids } },
      { $set: { ExternalCause: "Unknown", name: "Unknown" } }
    );
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
