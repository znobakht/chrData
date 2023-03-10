import { MongoClient } from "mongodb";
import { mongoURL, dbNameTemplate } from "./config/keys.js";

const ProcedureToUnknownFields = [
  "02",
  "05",
  "0c",
  "0d",
  "10",
  "20",
  "33",
  "34",
  "36",
  "42",
  "48",
  "unknow Protocol01",
  "unknow Protocol02",
  "unknow Protocol0a",
  "unknow Protocol14",
  "unknow Protocol16",
  "unknow Protocol19",
  "22",
  "unknow Protocol24",
  "unknow Protocol41",
  "unknow Protocol45",
  "unknow Protocol49",
];
let client; //if define it in the main function, catch doesnt have access to it.
async function main() {
  try {
    client = await MongoClient.connect(mongoURL);
    console.log("connected to db");
    for (let i = 5; i < 8; i++) {
      const db = client.db(`${dbNameTemplate}${i}`);
      const collection = db.collection("chr");
      await collection.updateMany(
        { "Procedure identification": { $in: ProcedureToUnknownFields } },
        { $set: { "Procedure identification": "Unknown" } }
      );
    }
    await client.close();
  } catch (err) {
    console.error(err);
    await client.close();
  }
}

console.log(new Date().getMinutes());
main()
  .then(() => console.log(new Date().getMinutes()))
  .catch((err) => {
    console.error(err);
  });