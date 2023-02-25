import { MongoClient } from "mongodb";
import { mongoURL } from "./config/keys.js";

const dbNameTemplate = "2022120";

const finalDB = "filters";
const finalCollectionName = "filter";

let client;
async function main() {
  try {
    client = await MongoClient.connect(mongoURL);
    console.log("connected to db");

    // const finalCollection = client.db(finalDB).collection(finalCollectionName);
    // for(let i =5; i<8; i++){
    for (let i = 6; i < 8; i++) {
      console.log(new Date().getHours());
      console.log(new Date().getMinutes());
      console.log(i);

      let dbName = `${dbNameTemplate}${i}`;
      const collection = client.db(dbName).collection("chr");

      const nodeName = "ESKZA";
      const forNameOfCollections = await collection
        .aggregate(
          [
            {
              $group: {
                _id: {
                  ChrType: "$ChrType",
                  AccessType: "$AccessType",
                  ProtocolCause: "$ProtocolCause",
                  ProcedureIdentification: "$Procedure identification",
                },
              },
            },
            {
              $project: {
                ChrType: "$_id.ChrType",
                AccessType: "$_id.AccessType",
                ProtocolCause: "$_id.ProtocolCause",
                ProcedureIdentification: "$_id.ProcedureIdentification",
                nodeName: nodeName,
              },
            },
            {
              $merge: {
                into: {
                  db: finalDB,
                  coll: finalCollectionName,
                },
                whenMatched: "keepExisting",
              },
            },
          ],
          { allowDiskUse: true }
        )
        .toArray();
    }

    await client.close();
  } catch (err) {
    console.error(err);
    await client.close();
  }
}
main()
  .then(() => console.log(new Date().getHours()))
  .then(() => console.log(new Date().getMinutes()))
  .catch((err) => {
    console.log(new Date().getMinutes());
    console.error(err);
  });
