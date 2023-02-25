import { MongoClient } from "mongodb";
import { mongoURL } from "../config/keys.js";

const dbNameTemplate = "2022120";

const finalDB = "newDBWithNewData1";

let client;
async function main() {
  try {
    client = await MongoClient.connect(mongoURL);
    console.log("connected to db");

    for (let i = 5; i < 8; i++) {
      console.log(new Date().getHours());
      console.log(new Date().getMinutes());
      console.log(i);

      let dbName = `${dbNameTemplate}${i}`;
      const collection = client.db(dbName).collection("chr");

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
          ],
          { allowDiskUse: true }
        )
        .toArray();

      for (let i = 0; i < forNameOfCollections.length; i++) {
        const collName = `ESKZA_${forNameOfCollections[i]._id.ChrType}_${forNameOfCollections[i]._id.AccessType}_${forNameOfCollections[i]._id.ProcedureIdentification}_${forNameOfCollections[i]._id.ProtocolCause}`;
        await collection
          .aggregate(
            [
              {
                $match: {
                  ChrType: forNameOfCollections[i]._id.ChrType,
                  AccessType: forNameOfCollections[i]._id.AccessType,
                  "Procedure identification":
                    forNameOfCollections[i]._id.ProcedureIdentification,
                  ProtocolCause: forNameOfCollections[i]._id.ProtocolCause,
                },
              },
              {
                $project: {
                  ChrType: 1,
                  AccessType: 1,
                  ProtocolCause: 1,
                  "Procedure identification": 1,
                  fieldName: "Inner cause",
                  "Inner cause": 1,
                  StartTime: {
                    $dateFromString: {
                      dateString: "$StartTime",
                      format: " %Y-%m-%d %H:%M:%S.%L",
                      onError: "$StartTime",
                    },
                  },
                },
              },
              {
                $project: {
                  ChrType: 1,
                  AccessType: 1,
                  ProtocolCause: 1,
                  "Procedure identification": 1,
                  fieldName: "Inner cause",
                  "Inner cause": 1,
                  StartTime: {
                    $dateFromString: {
                      dateString: "$StartTime",
                      format: " %Y-%m-%d %H:%M:%S",
                      onError: "$StartTime",
                    },
                  },
                },
              },
              {
                $project: {
                  ChrType: 1,
                  AccessType: 1,
                  ProtocolCause: 1,
                  "Procedure identification": 1,
                  fieldName: "Inner cause",
                  "Inner cause": 1,
                  StartTime: 1,
                  year: { $year: "$StartTime" },
                  month: { $month: "$StartTime" },
                  day: { $dayOfMonth: "$StartTime" },
                  hour: { $hour: "$StartTime" },
                },
              },
              {
                $group: {
                  _id: {
                    "Inner cause": "$Inner cause",
                    ChrType: "$ChrType",
                    AccessType: "$AccessType",
                    ProtocolCause: "$ProtocolCause",
                    ProcedureIdentification: "$Procedure identification",
                    year: "$year",
                    month: "$month",
                    day: "$day",
                    hour: "$hour",
                  },
                  name: { $first: "$Inner cause" },
                  fieldName: { $first: "$fieldName" },
                  value: { $sum: 1 },
                  year: { $first: "$year" },
                  month: { $first: "$month" },
                  day: { $first: "$day" },
                  hour: { $first: "$hour" },
                  ts: { $first: "$StartTime" },
                },
              },

              {
                $merge: {
                  into: {
                    db: finalDB,
                    coll: collName,
                  },
                  whenMatched: "keepExisting",
                },
              },
            ],
            { allowDiskUse: true }
          )
          .toArray();
      }
    }

    await client.close();
  } catch (err) {
    console.error(err);
    await client.close();
  }
}

// console.log(new Date().getHours());
// console.log(new Date().getMinutes());
main()
  .then(() => console.log(new Date().getMinutes()))
  .catch((err) => {
    console.log(new Date().getMinutes());
    console.error(err);
  });
