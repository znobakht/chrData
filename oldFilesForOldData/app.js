import { MongoClient } from "mongodb";
import { mongoURL } from "../config/keys.js";

const dbName1 = "20220628";
const tmpDB = "tmpDB";
const finalDB = "newDB";
let client;
async function main() {
  try {
    client = await MongoClient.connect(mongoURL);
    console.log("connected to db");

    const db = client.db(dbName1);
    let collections = await db.listCollections().toArray();
    // console.log(collections);

    const unwantedNames = [
      "chr",
      "locations",
      "procedure1_cause2",
      "timePicker",
      "removed_empty_fields",
    ];
    collections = collections.filter(
      (ele) => !unwantedNames.includes(ele.name)
    );

    let time = new Date();
    console.log(time.getMinutes());

    for (i = 0; i < collections.length; i++) {
      // for (let i = 0; i < 2; i++) {
      let collection = db.collection(collections[i].name);
      await collection
        .aggregate(
          [
            {
              $project: {
                fieldName: `${collections[i].name}`,
                value: 1,
                name: 1,
                ts: 1,
                ProtocolCause: 1,
                "Procedure identification": 1,
                _id: 0,
              },
            },
            { $sort: { ProtocolCause: 1, "Procedure identification": 1 } },
            { $out: { db: tmpDB, coll: "aggr" } },
          ],
          { allowDiskUse: true }
        )
        .toArray();
      let outputOfAggr = await client
        .db(tmpDB)
        .collection("aggr")
        .aggregate(
          [
            {
              $group: {
                _id: {
                  ProtocolCause: "$ProtocolCause",
                  ProcedureIdentification: "$Procedure identification",
                },
              },
            },
          ],
          { allowDiskUse: true }
        )
        .toArray();

      const totalSize = await client.db(tmpDB).collection("aggr").count();
      console.log(totalSize);

      for (let j = 0; j < outputOfAggr.length; j++) {
        // for(i = 0; i<1; i++){
        let name = `${outputOfAggr[j]._id.ProcedureIdentification}_${outputOfAggr[j]._id.ProtocolCause}`;
        const outs = await client
          .db(tmpDB)
          .collection("aggr")
          .aggregate(
            [
              {
                $match: {
                  ProtocolCause: `${outputOfAggr[j]._id.ProtocolCause}`,
                  "Procedure identification": `${outputOfAggr[j]._id.ProcedureIdentification}`,
                },
              },
              {
                $merge: {
                  into: { db: finalDB, coll: name },
                  whenMatched: "keepExisting",
                },
              },
            ],
            { allowDiskUse: true }
          )
          .toArray();
      }
    }

    time = new Date();
    console.log(time.getMinutes());
  } catch (err) {
    console.error(err);
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
  });
