import { MongoClient } from "mongodb";
import { mongoURL } from "./config/keys.js";

const dbName = "sampleData";

const tmpDB = "tmpDB";
const finalDB = "newDB";

let client;
async function main() {
  try {
    client = await MongoClient.connect(mongoURL);
    console.log("connected to db");
    const collection = client.db(dbName).collection("chr");
    // for(let i = 0; i<fieldNames.length; i++){

    const forNameOfCollections = await collection
      .aggregate([
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
        // {$out:{
        //     db: tmpDB, coll: "aggrTime"
        // }}
      ])
      .toArray();
    for (let i = 0; i < forNameOfCollections.length; i++) {
      const collName = `ESKZA_${forNameOfCollections[i]._id.ChrType}_${forNameOfCollections[i]._id.AccessType}_${forNameOfCollections[i]._id.ProcedureIdentification}_${forNameOfCollections[i]._id.ProtocolCause}`;
      await collection
        .aggregate([
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
              fieldName: "ExternalCause",
              ExternalCause: 1,
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
              fieldName: "ExternalCause",
              ExternalCause: 1,
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
              fieldName: "ExternalCause",
              ExternalCause: 1,
              year: { $year: "$StartTime" },
              month: { $month: "$StartTime" },
              day: { $dayOfMonth: "$StartTime" },
              hour: { $hour: "$StartTime" },
            },
          },
          {
            $group: {
              _id: {
                ExternalCause: "$ExternalCause",
                ChrType: "$ChrType",
                AccessType: "$AccessType",
                ProtocolCause: "$ProtocolCause",
                ProcedureIdentification: "$Procedure identification",
                year: "$year",
                month: "$month",
                day: "$day",
                hour: "$hour",
              },
              // "ExternalCause":{ "$first":"$ExternalCause"},
              name: { $first: "$ExternalCause" },
              // "Procedure identification": { "$first": "$Procedure identification" },
              // "ProtocolCause": { "$first": "$ProtocolCause" },
              // "ChrType":{ "$first": "$ChrType"},
              // "AccessType": {"$first":"$AccessType"},
              fieldName: { $first: "$fieldName" },
              value: { $sum: 1 },
              year: { $first: "$year" },
              month: { $first: "$month" },
              day: { $first: "$day" },
              hour: { $first: "$hour" },
            },
          },
          // {$sort:{ChrType :1, AccessType: 1, ProtocolCause: 1, ProcedureIdentification: 1, year: 1, month:1, day:1, hour:1, ExternalCause:1}},
          // {$sort:{ "$_id.hour":1, ExternalCause:1}},didnt work

          {
            $merge: {
              into: {
                db: tmpDB,
                coll: collName,
              },
              whenMatched: "keepExisting",
            },
          },
        ])
        .toArray();
    }

    // }
    // await collection.aggregate([
    //     { $project: {
    //         ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1,
    //         StartTime:  {
    //                 $dateFromString: { dateString: "$StartTime", format: " %Y-%m-%d %H:%M:%S.%L",
    //                 timezone: 'GMT', onError: "$StartTime"}
    //             }
    //     }},
    //     { $project: {
    //         ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1,
    //         StartTime:  {
    //                 $dateFromString: { dateString: "$StartTime", format: " %Y-%m-%d %H:%M:%S",
    //                 timezone: 'GMT', onError: "$StartTime"}
    //             }
    //     }},
    //     {$group:{
    //             _id:{
    //                 ChrType :"$ChrType",
    //                 AccessType: "$AccessType",
    //                 ProtocolCause: "$ProtocolCause",
    //                 ProcedureIdentification: "$Procedure identification",
    //                 year: { $year: "$StartTime" }, month:{ $month: "$StartTime" }, hour:{ $hour: "$StartTime" }
    //             },
    //             "Procedure identification": { "$first": "$Procedure identification" },
    //         "ProtocolCause": { "$first": "$ProtocolCause" },
    //         "ChrType":{ "$first": "$ChrType"},
    //         "AccessType": {"$first":"$AccessType"},
    //         }
    //     },
    //     {$sort:{ChrType :1, AccessType: 1, ProtocolCause: 1, ProcedureIdentification: 1, year: 1, month:1, day:1, hour:1}},

    //         {$out:{
    //             db: tmpDB, coll: "aggrTime"
    //         }}
    // ]).toArray();

    await client.close();
  } catch (err) {
    console.error(err);
    await client.close();
  }
}

console.log(new Date().getMinutes());
main().then(() => console.log(new Date().getMinutes()));
