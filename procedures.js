const mongoClient = require("mongodb").MongoClient;
const mongoURL = require("./config/keys").mongoURL;

const srcDbNameTmp = "2022120";
const destDbName = "ProceduresCount";

const collectionName = "chr";

let client;
async function main() {
  try {
    client = await mongoClient.connect(mongoURL);
    console.log("connected to mongodb server");

    for (let i = 5; i < 8; i++) {
      console.log(i);
      const dbName = `${srcDbNameTmp}${i}`;
      const collection = client.db(dbName).collection(collectionName);

      await collection
        .aggregate(
          [
            {
              $project: {
                "Procedure identification": 1,
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
                StartTime: 1,
                year: { $year: $StartTime },
                month: { $month: $StartTime },
                day: { $dayOfMonth: $StartTime },
                hour: { $hour: $StartTime },
              },
            },
            {
              $group: {
                _id: {
                  ProcedureIdentification: "$Procedure identification",
                  year: "$year",
                  month: "$month",
                  day: "$day",
                  hour: "$hour",
                },
                ProcedureIdentification: {
                  $first: "$Procedure identification",
                },
                ts: { $first: "$StartTime" },
                count: { $sum: 1 },
              },
            },
            {
              $project: {
                // _id: 0,
                count: 1,
                ProcedureIdentification: 1,
                withHour: {
                  $dateToString: { format: "%Y-%m-%d %H", date: "$ts" },
                },
                ts: 1,
              },
            },
            {
              $project: {
                ProcedureIdentification: 1,
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
                  coll: destDbName,
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
main();
