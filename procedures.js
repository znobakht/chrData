const mongoClient = require("mongodb").MongoClient;
const mongoURL = require("./config/keys").mongoURL;

const srcDbNameTmp = "2022120";
const destDbName = "filters";
const tmpCollectionName = "tmpDB";
const destProcedureCollectionName = "ProceduresCount";
const destProtocolBasedProcedureCollectionName = "ProtocolBasedProcedureCount";

const collectionName = "chr";

let client;
async function main() {
  try {
    client = await mongoClient.connect(mongoURL);
    console.log("connected to mongodb server");

    for (let i = 5; i < 8; i++) {
      console.log(i);
      printTime();
      const dbName = `${srcDbNameTmp}${i}`;
      const collection = client.db(dbName).collection(collectionName);

      await collection
        .aggregate(
          [
            {
              $project: {
                "Procedure identification": 1,
                ChrType: 1,
                AccessType: 1,
                ProtocolCause: 1,
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
                "Procedure identification": 1,
                ChrType: 1,
                AccessType: 1,
                ProtocolCause: 1,
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
                "Procedure identification": 1,
                ChrType: 1,
                AccessType: 1,
                ProtocolCause: 1,
                StartTime: 1,
                year: { $year: "$StartTime" },
                month: { $month: "$StartTime" },
                day: { $dayOfMonth: "$StartTime" },
                hour: { $hour: "$StartTime" },
              },
            },
            {
              $out: {
                db: destDbName,
                coll: tmpCollectionName,
              },
            },
          ],
          { allowDiskUse: true }
        )
        .toArray();

      console.log(`procedures of ${i}`);
      printTime();

      const tmpCollection = client.db(destDbName).collection(tmpCollectionName);
      await tmpCollection
        .aggregate(
          [
            {
              $group: {
                _id: {
                  ChrType: "$ChrType",
                  AccessType: "$AccessType",
                  ProcedureIdentification: "$Procedure identification",
                  year: "$year",
                  month: "$month",
                  day: "$day",
                  hour: "$hour",
                },
                ProcedureIdentification: {
                  $first: "$Procedure identification",
                },
                AccessType: {
                  $first: "$AccessType",
                },
                ChrType: {
                  $first: "$ChrType",
                },
                ts: { $first: "$StartTime" },
                count: { $sum: 1 },
              },
            },
            {
              $project: {
                count: 1,
                ProcedureIdentification: 1,
                ChrType: 1,
                AccessType: 1,
                withHour: {
                  $dateToString: { format: "%Y-%m-%d %H", date: "$ts" },
                },
                ts: 1,
              },
            },
            {
              $project: {
                ProcedureIdentification: 1,
                ChrType: 1,
                AccessType: 1,
                count: 1,
                ts: {
                  $dateFromString: {
                    dateString: "$withHour",
                    format: "%Y-%m-%d %H",
                    onError: "$ts",
                  },
                },
              },
            },
            {
              $merge: {
                into: {
                  db: destDbName,
                  coll: destProcedureCollectionName,
                },
                whenMatched: "keepExisting",
              },
            },
          ],
          { allowDiskUse: true }
        )
        .toArray();

      console.log(`protocols of ${i}`);
      printTime();

      await tmpCollection
        .aggregate(
          [
            {
              $group: {
                _id: {
                  ChrType: "$ChrType",
                  AccessType: "$AccessType",
                  ProcedureIdentification: "$Procedure identification",
                  ProtocolCause: "$ProtocolCause",
                  year: "$year",
                  month: "$month",
                  day: "$day",
                  hour: "$hour",
                },
                ProcedureIdentification: {
                  $first: "$Procedure identification",
                },
                AccessType: {
                  $first: "$AccessType",
                },
                ChrType: {
                  $first: "$ChrType",
                },
                ProtocolCause: {
                  $first: "$ProtocolCause",
                },
                ts: { $first: "$StartTime" },
                count: { $sum: 1 },
              },
            },
            {
              $project: {
                count: 1,
                ProcedureIdentification: 1,
                ChrType: 1,
                ProtocolCause: 1,
                AccessType: 1,
                withHour: {
                  $dateToString: { format: "%Y-%m-%d %H", date: "$ts" },
                },
                ts: 1,
              },
            },
            {
              $project: {
                ProcedureIdentification: 1,
                ChrType: 1,
                AccessType: 1,
                ProtocolCause: 1,
                count: 1,
                ts: {
                  $dateFromString: {
                    dateString: "$withHour",
                    format: "%Y-%m-%d %H",
                    onError: "$ts",
                  },
                },
              },
            },
            {
              $merge: {
                into: {
                  db: destDbName,
                  coll: destProtocolBasedProcedureCollectionName,
                },
                whenMatched: "keepExisting",
              },
            },
          ],
          { allowDiskUse: true }
        )
        .toArray();
    }

    client.close();
  } catch (err) {
    console.log(err);
    console.error(err);
    client.close();
  }
}

main()
  .then(() => {
    printTime();
    // console.log(new Date().getHours());
    // console.log(new Date().getMinutes());
  })
  .catch((err) => {
    printTime();
    console.error(err);
  });

function printTime() {
  let date = new Date();
  console.log(date.getHours());
  console.log(date.getMinutes());
}
