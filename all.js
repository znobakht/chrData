const mongoClient = require("mongodb").MongoClient;
const dbUrl = "mongodb://root:Password!123@192.168.238.10:27017";

const dbNameTemplate = "2022120"

const finalDB = 'newDBWithNewData11';
const tmpCollectionName = "tmpColl";

let client;
async function main(){
    try{
        client = await mongoClient.connect(dbUrl);
        console.log("connected to db");

        for(let i =5; i<8; i++){
        // for(let i =5; i<8; i++){
            console.log(new Date().getHours());
            console.log(new Date().getMinutes());
            console.log(i)

            let dbName = `${dbNameTemplate}${i}`;
            const collection = client.db(dbName).collection("chr");

            const forNameOfCollections = await collection.aggregate([
                {$group:{
                        _id:{
                            ChrType :"$ChrType",
                            AccessType: "$AccessType",
                            ProtocolCause: "$ProtocolCause", 
                            ProcedureIdentification: "$Procedure identification",
                        },
                    }
                },
            ], { "allowDiskUse": true }).toArray();
            console.log(forNameOfCollections.length)
    
            for(let i =0; i<forNameOfCollections.length; i++){
            // for(let i =0; i<1; i++){
                console.log(i)
                const collName = `ESKZA_${forNameOfCollections[i]._id.ChrType}_${forNameOfCollections[i]._id.AccessType}_${forNameOfCollections[i]._id.ProcedureIdentification}_${forNameOfCollections[i]._id.ProtocolCause}`
                await collection.aggregate([
                    {
                        $match:{
                            ChrType:forNameOfCollections[i]._id.ChrType,
                            AccessType:forNameOfCollections[i]._id.AccessType,
                            "Procedure identification":forNameOfCollections[i]._id.ProcedureIdentification,
                            ProtocolCause:forNameOfCollections[i]._id.ProtocolCause,
                        }
                    },
                    { $project: { 
                        // ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Inner cause":1,
                        // ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, fieldName:"Inner cause","Inner cause":1,
                        "IMSI":1,"AccessType":1,"Procedure identification":1,"ProtocolCause":1,"ExternalCause":1,"Inner cause":1,"APN In Used":1,"APN NI":1,"APN NI in used":1,"APN Request by MS":1,"Authentication Flag":1,"Bearer ID":1,"BSSID":1,"ChrType":1,"CI":1,"CUR TAC":1,"DelayTime":1,"eNB ID":1,"EPS attach result":1,"GGSN Address for signalling":1,"GGSN Address for user traffic":1,"Handover Cancel Phase":1,"IMEI check Flag":1,"IMEISV":1,"Init proc cause":1,"Internal Physical Location":1,"LAC":1,"Message Type":1,"MSISDN":1,"New GUTI MMEC":1,"New GUTI MMEGI":1,"Old GUTI MMEC":1,"Old GUTI MMEGI":1,"Old LAC":1,"Old RAC":1,"Old TAC":1,"PDN Address in used":1,"PDN Gateway Address":1,"PDN Type":1,"PDP address in use":1,"PDP type in used":1,"PDP type request by MS":1,"Peer SGSN Address":1,"QCI":1,"RAC":1,"RNC ID":1,"S1-U eNodeB Address1":1,"S1-U Serving Gateway Address1":1,"Serving Gateway Address":1,"SGS RePaging Count":1,"SGW Change Flag":1,"SRVCC Cause":1,"SRVCC Preparation Period":1,"State MachineName":1,"Target RNC ID":1,"Target TAC":1,"Traffic Class":1,"User Home PLMN":1,"VLR No_":1,"Voice domain preference and UE's usage setting":1,
                        StartTime:  { 
                                $dateFromString: { dateString: "$StartTime", format: " %Y-%m-%d %H:%M:%S.%L", onError: "$StartTime"} 
                            } 
                    }},
                    { $project: { 
                        // ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Inner cause":1,
                        // ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, fieldName:"Inner cause","Inner cause":1,
                        "IMSI":1,"AccessType":1,"Procedure identification":1,"ProtocolCause":1,"ExternalCause":1,"Inner cause":1,"APN In Used":1,"APN NI":1,"APN NI in used":1,"APN Request by MS":1,"Authentication Flag":1,"Bearer ID":1,"BSSID":1,"ChrType":1,"CI":1,"CUR TAC":1,"DelayTime":1,"eNB ID":1,"EPS attach result":1,"GGSN Address for signalling":1,"GGSN Address for user traffic":1,"Handover Cancel Phase":1,"IMEI check Flag":1,"IMEISV":1,"Init proc cause":1,"Internal Physical Location":1,"LAC":1,"Message Type":1,"MSISDN":1,"New GUTI MMEC":1,"New GUTI MMEGI":1,"Old GUTI MMEC":1,"Old GUTI MMEGI":1,"Old LAC":1,"Old RAC":1,"Old TAC":1,"PDN Address in used":1,"PDN Gateway Address":1,"PDN Type":1,"PDP address in use":1,"PDP type in used":1,"PDP type request by MS":1,"Peer SGSN Address":1,"QCI":1,"RAC":1,"RNC ID":1,"S1-U eNodeB Address1":1,"S1-U Serving Gateway Address1":1,"Serving Gateway Address":1,"SGS RePaging Count":1,"SGW Change Flag":1,"SRVCC Cause":1,"SRVCC Preparation Period":1,"State MachineName":1,"Target RNC ID":1,"Target TAC":1,"Traffic Class":1,"User Home PLMN":1,"VLR No_":1,"Voice domain preference and UE's usage setting":1,
                        StartTime:  { 
                                $dateFromString: { dateString: "$StartTime", format: " %Y-%m-%d %H:%M:%S", onError: "$StartTime"} 
                            } 
                    }},
                    { $project: { 
                        // ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Inner cause":1,
                        "IMSI":1,"AccessType":1,"Procedure identification":1,"ProtocolCause":1,"ExternalCause":1,"Inner cause":1,"APN In Used":1,"APN NI":1,"APN NI in used":1,"APN Request by MS":1,"Authentication Flag":1,"Bearer ID":1,"BSSID":1,"ChrType":1,"CI":1,"CUR TAC":1,"DelayTime":1,"eNB ID":1,"EPS attach result":1,"GGSN Address for signalling":1,"GGSN Address for user traffic":1,"Handover Cancel Phase":1,"IMEI check Flag":1,"IMEISV":1,"Init proc cause":1,"Internal Physical Location":1,"LAC":1,"Message Type":1,"MSISDN":1,"New GUTI MMEC":1,"New GUTI MMEGI":1,"Old GUTI MMEC":1,"Old GUTI MMEGI":1,"Old LAC":1,"Old RAC":1,"Old TAC":1,"PDN Address in used":1,"PDN Gateway Address":1,"PDN Type":1,"PDP address in use":1,"PDP type in used":1,"PDP type request by MS":1,"Peer SGSN Address":1,"QCI":1,"RAC":1,"RNC ID":1,"S1-U eNodeB Address1":1,"S1-U Serving Gateway Address1":1,"Serving Gateway Address":1,"SGS RePaging Count":1,"SGW Change Flag":1,"SRVCC Cause":1,"SRVCC Preparation Period":1,"State MachineName":1,"Target RNC ID":1,"Target TAC":1,"Traffic Class":1,"User Home PLMN":1,"VLR No_":1,"Voice domain preference and UE's usage setting":1,
                        "StartTime":1,
                        year: { $year: "$StartTime" }, month:{ $month: "$StartTime" }, day:{$dayOfMonth: "$StartTime"},hour:{ $hour: "$StartTime" }
                    }},
                        
                    {$merge:{ into:{
                        db: finalDB, coll: tmpCollectionName
                    }, whenMatched: "keepExisting"}}
                ], { "allowDiskUse": true }).toArray();

                console.log("second stage")

                const tmpCollection = client.db(finalDB).collection(tmpCollectionName);
            // await tmpCollection.aggregate([
            //         { $project: { 
            //             ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Inner cause":1, "StartTime":1,fieldName:"Inner cause",
            //             year: 1, month:1, day:1,hour:1
            //         }},
            //         {$group:{
            //             _id:{
            //                 "Inner cause":"$Inner cause",
            //                 ChrType :"$ChrType",
            //                 AccessType: "$AccessType",
            //                 ProtocolCause: "$ProtocolCause", 
            //                 ProcedureIdentification: "$Procedure identification",
            //                 year: "$year", month: "$month", day: "$day", hour:"$hour" 
            //             },
            //             "value":{ "$first":"$Inner cause"},
            //             "fieldName":{ "$first":"$fieldName"},
            //             count: { "$sum": 1 },
            //             "ts":{"$first":"$StartTime"}
            //         }
            //     },
            //     {$project:{
            //         _id:0,
            //         "value":1,
            //             "fieldName":1,
            //             count: 1,
            //         withHour:{ $dateToString: { format: "%Y-%m-%d %H", date: "$ts" } },
            //         ts:1
            //     }},
            //     {$project:{
            //         "value":1,
            //             "fieldName":1,
            //             count: 1,
            //         ts:{ $dateFromString: { dateString: "$withHour", format: "%Y-%m-%d %H", onError: "$ts"}  },
            //     }},
                    
            //     {$merge:{ into:{
            //         db: finalDB, coll: collName
            //     }, whenMatched: "keepExisting"}}
            // ], { "allowDiskUse": true }).toArray();
            // await tmpCollection.aggregate([
            //     { $project: { 
            //         ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN In Used":1, "StartTime":1,fieldName:"APN In Used",
            //         year: 1, month:1, day:1,hour:1
            //     }},
            //     {$group:{
            //         _id:{
            //             "APN In Used":"$APN In Used",
            //             ChrType :"$ChrType",
            //             AccessType: "$AccessType",
            //             ProtocolCause: "$ProtocolCause", 
            //             ProcedureIdentification: "$Procedure identification",
            //             year: "$year", month: "$month", day: "$day", hour:"$hour" 
            //         },
            //         "value":{ "$first":"$APN In Used"},
            //         "fieldName":{ "$first":"$fieldName"},
            //         count: { "$sum": 1 },
            //         "ts":{"$first":"$StartTime"}
            //     }
            // },
            // {$project:{
            //     _id:0,
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     withHour:{ $dateToString: { format: "%Y-%m-%d %H", date: "$ts" } },
            //     ts:1
            // }},
            // {$project:{
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     ts:{ $dateFromString: { dateString: "$withHour", format: "%Y-%m-%d %H", onError: "$ts"}  },
            // }},
                
            // {$merge:{ into:{
            //     db: finalDB, coll: collName
            // }, whenMatched: "keepExisting"}}
            // ], { "allowDiskUse": true }).toArray();
            // await tmpCollection.aggregate([
            //     { $project: { 
            //         ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN NI":1, "StartTime":1,fieldName:"APN NI",
            //         year: 1, month:1, day:1,hour:1
            //     }},
            //     {$group:{
            //         _id:{
            //             "APN NI":"$APN NI",
            //             ChrType :"$ChrType",
            //             AccessType: "$AccessType",
            //             ProtocolCause: "$ProtocolCause", 
            //             ProcedureIdentification: "$Procedure identification",
            //             year: "$year", month: "$month", day: "$day", hour:"$hour" 
            //         },
            //         "value":{ "$first":"$APN NI"},
            //         "fieldName":{ "$first":"$fieldName"},
            //         count: { "$sum": 1 },
            //         "ts":{"$first":"$StartTime"}
            //     }
            // },
            // {$project:{
            //     _id:0,
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     withHour:{ $dateToString: { format: "%Y-%m-%d %H", date: "$ts" } },
            //     ts:1
            // }},
            // {$project:{
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     ts:{ $dateFromString: { dateString: "$withHour", format: "%Y-%m-%d %H", onError: "$ts"}  },
            // }},
                
            // {$merge:{ into:{
            //     db: finalDB, coll: collName
            // }, whenMatched: "keepExisting"}}
            // ], { "allowDiskUse": true }).toArray();
            // await tmpCollection.aggregate([
            //     { $project: { 
            //         ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "IMSI":1, "StartTime":1,fieldName:"IMSI",
            //         year: 1, month:1, day:1,hour:1
            //     }},
            //     {$group:{
            //         _id:{
            //             "IMSI":"$IMSI",
            //             ChrType :"$ChrType",
            //             AccessType: "$AccessType",
            //             ProtocolCause: "$ProtocolCause", 
            //             ProcedureIdentification: "$Procedure identification",
            //             year: "$year", month: "$month", day: "$day", hour:"$hour" 
            //         },
            //         "value":{ "$first":"$IMSI"},
            //         "fieldName":{ "$first":"$fieldName"},
            //         count: { "$sum": 1 },
            //         "ts":{"$first":"$StartTime"}
            //     }
            // },
            // {$project:{
            //     _id:0,
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     withHour:{ $dateToString: { format: "%Y-%m-%d %H", date: "$ts" } },
            //     ts:1
            // }},
            // {$project:{
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     ts:{ $dateFromString: { dateString: "$withHour", format: "%Y-%m-%d %H", onError: "$ts"}  },
            // }},
                
            // {$merge:{ into:{
            //     db: finalDB, coll: collName
            // }, whenMatched: "keepExisting"}}
            // ], { "allowDiskUse": true }).toArray();
            // await tmpCollection.aggregate([
            //     { $project: { 
            //         ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "ExternalCause":1, "StartTime":1,fieldName:"ExternalCause",
            //         year: 1, month:1, day:1,hour:1
            //     }},
            //     {$group:{
            //         _id:{
            //             "ExternalCause":"$ExternalCause",
            //             ChrType :"$ChrType",
            //             AccessType: "$AccessType",
            //             ProtocolCause: "$ProtocolCause", 
            //             ProcedureIdentification: "$Procedure identification",
            //             year: "$year", month: "$month", day: "$day", hour:"$hour" 
            //         },
            //         "value":{ "$first":"$ExternalCause"},
            //         "fieldName":{ "$first":"$fieldName"},
            //         count: { "$sum": 1 },
            //         "ts":{"$first":"$StartTime"}
            //     }
            // },
            // {$project:{
            //     _id:0,
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     withHour:{ $dateToString: { format: "%Y-%m-%d %H", date: "$ts" } },
            //     ts:1
            // }},
            // {$project:{
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     ts:{ $dateFromString: { dateString: "$withHour", format: "%Y-%m-%d %H", onError: "$ts"}  },
            // }},
                
            // {$merge:{ into:{
            //     db: finalDB, coll: collName
            // }, whenMatched: "keepExisting"}}
            // ], { "allowDiskUse": true }).toArray();
            // await tmpCollection.aggregate([
            //     { $project: { 
            //         ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN NI in used":1, "StartTime":1,fieldName:"APN NI in used",
            //         year: 1, month:1, day:1,hour:1
            //     }},
            //     {$group:{
            //         _id:{
            //             "APN NI in used":"$APN NI in used",
            //             ChrType :"$ChrType",
            //             AccessType: "$AccessType",
            //             ProtocolCause: "$ProtocolCause", 
            //             ProcedureIdentification: "$Procedure identification",
            //             year: "$year", month: "$month", day: "$day", hour:"$hour" 
            //         },
            //         "value":{ "$first":"$APN NI in used"},
            //         "fieldName":{ "$first":"$fieldName"},
            //         count: { "$sum": 1 },
            //         "ts":{"$first":"$StartTime"}
            //     }
            // },
            // {$project:{
            //     _id:0,
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     withHour:{ $dateToString: { format: "%Y-%m-%d %H", date: "$ts" } },
            //     ts:1
            // }},
            // {$project:{
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     ts:{ $dateFromString: { dateString: "$withHour", format: "%Y-%m-%d %H", onError: "$ts"}  },
            // }},
                
            // {$merge:{ into:{
            //     db: finalDB, coll: collName
            // }, whenMatched: "keepExisting"}}
            // ], { "allowDiskUse": true }).toArray();
            // await tmpCollection.aggregate([
            //     { $project: { 
            //         ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN Request by MS":1, "StartTime":1,fieldName:"APN Request by MS",
            //         year: 1, month:1, day:1,hour:1
            //     }},
            //     {$group:{
            //         _id:{
            //             "APN Request by MS":"$APN Request by MS",
            //             ChrType :"$ChrType",
            //             AccessType: "$AccessType",
            //             ProtocolCause: "$ProtocolCause", 
            //             ProcedureIdentification: "$Procedure identification",
            //             year: "$year", month: "$month", day: "$day", hour:"$hour" 
            //         },
            //         "value":{ "$first":"$APN Request by MS"},
            //         "fieldName":{ "$first":"$fieldName"},
            //         count: { "$sum": 1 },
            //         "ts":{"$first":"$StartTime"}
            //     }
            // },
            // {$project:{
            //     _id:0,
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     withHour:{ $dateToString: { format: "%Y-%m-%d %H", date: "$ts" } },
            //     ts:1
            // }},
            // {$project:{
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     ts:{ $dateFromString: { dateString: "$withHour", format: "%Y-%m-%d %H", onError: "$ts"}  },
            // }},
                
            // {$merge:{ into:{
            //     db: finalDB, coll: collName
            // }, whenMatched: "keepExisting"}}
            // ], { "allowDiskUse": true }).toArray();
            // await tmpCollection.aggregate([
            //     { $project: { 
            //         ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Authentication Flag":1, "StartTime":1,fieldName:"Authentication Flag",
            //         year: 1, month:1, day:1,hour:1
            //     }},
            //     {$group:{
            //         _id:{
            //             "Authentication Flag":"$Authentication Flag",
            //             ChrType :"$ChrType",
            //             AccessType: "$AccessType",
            //             ProtocolCause: "$ProtocolCause", 
            //             ProcedureIdentification: "$Procedure identification",
            //             year: "$year", month: "$month", day: "$day", hour:"$hour" 
            //         },
            //         "value":{ "$first":"$Authentication Flag"},
            //         "fieldName":{ "$first":"$fieldName"},
            //         count: { "$sum": 1 },
            //         "ts":{"$first":"$StartTime"}
            //     }
            // },
            // {$project:{
            //     _id:0,
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     withHour:{ $dateToString: { format: "%Y-%m-%d %H", date: "$ts" } },
            //     ts:1
            // }},
            // {$project:{
            //     "value":1,
            //         "fieldName":1,
            //         count: 1,
            //     ts:{ $dateFromString: { dateString: "$withHour", format: "%Y-%m-%d %H", onError: "$ts"}  },
            // }},
                
            // {$merge:{ into:{
            //     db: finalDB, coll: collName
            // }, whenMatched: "keepExisting"}}
            // ], { "allowDiskUse": true }).toArray();


            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "IMSI":1, "StartTime":1,fieldName:"IMSI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "IMSI":"$IMSI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$IMSI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "AccessType":1, "StartTime":1,fieldName:"AccessType",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "AccessType":"$AccessType",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$AccessType"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "ExternalCause":1, "StartTime":1,fieldName:"ExternalCause",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "ExternalCause":"$ExternalCause",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$ExternalCause"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Inner cause":1, "StartTime":1,fieldName:"Inner cause",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Inner cause":"$Inner cause",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Inner cause"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN In Used":1, "StartTime":1,fieldName:"APN In Used",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "APN In Used":"$APN In Used",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$APN In Used"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN NI":1, "StartTime":1,fieldName:"APN NI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "APN NI":"$APN NI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$APN NI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN NI in used":1, "StartTime":1,fieldName:"APN NI in used",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "APN NI in used":"$APN NI in used",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$APN NI in used"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN Request by MS":1, "StartTime":1,fieldName:"APN Request by MS",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "APN Request by MS":"$APN Request by MS",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$APN Request by MS"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Authentication Flag":1, "StartTime":1,fieldName:"Authentication Flag",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Authentication Flag":"$Authentication Flag",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Authentication Flag"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Bearer ID":1, "StartTime":1,fieldName:"Bearer ID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Bearer ID":"$Bearer ID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Bearer ID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "BSSID":1, "StartTime":1,fieldName:"BSSID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "BSSID":"$BSSID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$BSSID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "CI":1, "StartTime":1,fieldName:"CI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "CI":"$CI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$CI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "CUR TAC":1, "StartTime":1,fieldName:"CUR TAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "CUR TAC":"$CUR TAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$CUR TAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "DelayTime":1, "StartTime":1,fieldName:"DelayTime",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "DelayTime":"$DelayTime",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$DelayTime"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "eNB ID":1, "StartTime":1,fieldName:"eNB ID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "eNB ID":"$eNB ID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$eNB ID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "EPS attach result":1, "StartTime":1,fieldName:"EPS attach result",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "EPS attach result":"$EPS attach result",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$EPS attach result"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "GGSN Address for signalling":1, "StartTime":1,fieldName:"GGSN Address for signalling",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "GGSN Address for signalling":"$GGSN Address for signalling",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$GGSN Address for signalling"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "GGSN Address for user traffic":1, "StartTime":1,fieldName:"GGSN Address for user traffic",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "GGSN Address for user traffic":"$GGSN Address for user traffic",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$GGSN Address for user traffic"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Handover Cancel Phase":1, "StartTime":1,fieldName:"Handover Cancel Phase",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Handover Cancel Phase":"$Handover Cancel Phase",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Handover Cancel Phase"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "IMEI check Flag":1, "StartTime":1,fieldName:"IMEI check Flag",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "IMEI check Flag":"$IMEI check Flag",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$IMEI check Flag"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "IMEISV":1, "StartTime":1,fieldName:"IMEISV",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "IMEISV":"$IMEISV",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$IMEISV"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Init proc cause":1, "StartTime":1,fieldName:"Init proc cause",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Init proc cause":"$Init proc cause",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Init proc cause"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Internal Physical Location":1, "StartTime":1,fieldName:"Internal Physical Location",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Internal Physical Location":"$Internal Physical Location",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Internal Physical Location"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "LAC":1, "StartTime":1,fieldName:"LAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "LAC":"$LAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$LAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Message Type":1, "StartTime":1,fieldName:"Message Type",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Message Type":"$Message Type",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Message Type"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "MSISDN":1, "StartTime":1,fieldName:"MSISDN",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "MSISDN":"$MSISDN",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$MSISDN"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "New GUTI MMEC":1, "StartTime":1,fieldName:"New GUTI MMEC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "New GUTI MMEC":"$New GUTI MMEC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$New GUTI MMEC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "New GUTI MMEGI":1, "StartTime":1,fieldName:"New GUTI MMEGI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "New GUTI MMEGI":"$New GUTI MMEGI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$New GUTI MMEGI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old GUTI MMEC":1, "StartTime":1,fieldName:"Old GUTI MMEC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old GUTI MMEC":"$Old GUTI MMEC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old GUTI MMEC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old GUTI MMEGI":1, "StartTime":1,fieldName:"Old GUTI MMEGI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old GUTI MMEGI":"$Old GUTI MMEGI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old GUTI MMEGI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old LAC":1, "StartTime":1,fieldName:"Old LAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old LAC":"$Old LAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old LAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old RAC":1, "StartTime":1,fieldName:"Old RAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old RAC":"$Old RAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old RAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old TAC":1, "StartTime":1,fieldName:"Old TAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old TAC":"$Old TAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old TAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDN Address in used":1, "StartTime":1,fieldName:"PDN Address in used",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDN Address in used":"$PDN Address in used",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDN Address in used"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDN Gateway Address":1, "StartTime":1,fieldName:"PDN Gateway Address",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDN Gateway Address":"$PDN Gateway Address",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDN Gateway Address"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDN Type":1, "StartTime":1,fieldName:"PDN Type",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDN Type":"$PDN Type",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDN Type"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDP address in use":1, "StartTime":1,fieldName:"PDP address in use",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDP address in use":"$PDP address in use",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDP address in use"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDP type in used":1, "StartTime":1,fieldName:"PDP type in used",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDP type in used":"$PDP type in used",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDP type in used"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDP type request by MS":1, "StartTime":1,fieldName:"PDP type request by MS",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDP type request by MS":"$PDP type request by MS",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDP type request by MS"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Peer SGSN Address":1, "StartTime":1,fieldName:"Peer SGSN Address",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Peer SGSN Address":"$Peer SGSN Address",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Peer SGSN Address"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "QCI":1, "StartTime":1,fieldName:"QCI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "QCI":"$QCI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$QCI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "RAC":1, "StartTime":1,fieldName:"RAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "RAC":"$RAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$RAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "RNC ID":1, "StartTime":1,fieldName:"RNC ID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "RNC ID":"$RNC ID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$RNC ID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "S1-U eNodeB Address1":1, "StartTime":1,fieldName:"S1-U eNodeB Address1",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "S1-U eNodeB Address1":"$S1-U eNodeB Address1",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$S1-U eNodeB Address1"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "S1-U Serving Gateway Address1":1, "StartTime":1,fieldName:"S1-U Serving Gateway Address1",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "S1-U Serving Gateway Address1":"$S1-U Serving Gateway Address1",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$S1-U Serving Gateway Address1"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Serving Gateway Address":1, "StartTime":1,fieldName:"Serving Gateway Address",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Serving Gateway Address":"$Serving Gateway Address",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Serving Gateway Address"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "SGS RePaging Count":1, "StartTime":1,fieldName:"SGS RePaging Count",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "SGS RePaging Count":"$SGS RePaging Count",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$SGS RePaging Count"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "SGW Change Flag":1, "StartTime":1,fieldName:"SGW Change Flag",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "SGW Change Flag":"$SGW Change Flag",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$SGW Change Flag"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "SRVCC Cause":1, "StartTime":1,fieldName:"SRVCC Cause",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "SRVCC Cause":"$SRVCC Cause",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$SRVCC Cause"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "SRVCC Preparation Period":1, "StartTime":1,fieldName:"SRVCC Preparation Period",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "SRVCC Preparation Period":"$SRVCC Preparation Period",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$SRVCC Preparation Period"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "State MachineName":1, "StartTime":1,fieldName:"State MachineName",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "State MachineName":"$State MachineName",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$State MachineName"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Target RNC ID":1, "StartTime":1,fieldName:"Target RNC ID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Target RNC ID":"$Target RNC ID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Target RNC ID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Target TAC":1, "StartTime":1,fieldName:"Target TAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Target TAC":"$Target TAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Target TAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Traffic Class":1, "StartTime":1,fieldName:"Traffic Class",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Traffic Class":"$Traffic Class",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Traffic Class"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "User Home PLMN":1, "StartTime":1,fieldName:"User Home PLMN",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "User Home PLMN":"$User Home PLMN",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$User Home PLMN"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "VLR No_":1, "StartTime":1,fieldName:"VLR No_",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "VLR No_":"$VLR No_",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$VLR No_"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Voice domain preference and UE's usage setting":1, "StartTime":1,fieldName:"Voice domain preference and UE's usage setting",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Voice domain preference and UE's usage setting":"$Voice domain preference and UE's usage setting",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Voice domain preference and UE's usage setting"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "IMSI":1, "StartTime":1,fieldName:"IMSI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "IMSI":"$IMSI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$IMSI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Procedure identification":1, "StartTime":1,fieldName:"Procedure identification",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Procedure identification":"$Procedure identification",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Procedure identification"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "ProtocolCause":1, "StartTime":1,fieldName:"ProtocolCause",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "ProtocolCause":"$ProtocolCause",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$ProtocolCause"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "ExternalCause":1, "StartTime":1,fieldName:"ExternalCause",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "ExternalCause":"$ExternalCause",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$ExternalCause"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Inner cause":1, "StartTime":1,fieldName:"Inner cause",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Inner cause":"$Inner cause",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Inner cause"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN In Used":1, "StartTime":1,fieldName:"APN In Used",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "APN In Used":"$APN In Used",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$APN In Used"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN NI":1, "StartTime":1,fieldName:"APN NI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "APN NI":"$APN NI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$APN NI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN NI in used":1, "StartTime":1,fieldName:"APN NI in used",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "APN NI in used":"$APN NI in used",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$APN NI in used"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "APN Request by MS":1, "StartTime":1,fieldName:"APN Request by MS",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "APN Request by MS":"$APN Request by MS",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$APN Request by MS"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Authentication Flag":1, "StartTime":1,fieldName:"Authentication Flag",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Authentication Flag":"$Authentication Flag",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Authentication Flag"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Bearer ID":1, "StartTime":1,fieldName:"Bearer ID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Bearer ID":"$Bearer ID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Bearer ID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "BSSID":1, "StartTime":1,fieldName:"BSSID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "BSSID":"$BSSID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$BSSID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "ChrType":1, "StartTime":1,fieldName:"ChrType",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "ChrType":"$ChrType",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$ChrType"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "CI":1, "StartTime":1,fieldName:"CI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "CI":"$CI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$CI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "CUR TAC":1, "StartTime":1,fieldName:"CUR TAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "CUR TAC":"$CUR TAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$CUR TAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "DelayTime":1, "StartTime":1,fieldName:"DelayTime",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "DelayTime":"$DelayTime",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$DelayTime"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "eNB ID":1, "StartTime":1,fieldName:"eNB ID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "eNB ID":"$eNB ID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$eNB ID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "EPS attach result":1, "StartTime":1,fieldName:"EPS attach result",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "EPS attach result":"$EPS attach result",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$EPS attach result"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "GGSN Address for signalling":1, "StartTime":1,fieldName:"GGSN Address for signalling",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "GGSN Address for signalling":"$GGSN Address for signalling",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$GGSN Address for signalling"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "GGSN Address for user traffic":1, "StartTime":1,fieldName:"GGSN Address for user traffic",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "GGSN Address for user traffic":"$GGSN Address for user traffic",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$GGSN Address for user traffic"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Handover Cancel Phase":1, "StartTime":1,fieldName:"Handover Cancel Phase",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Handover Cancel Phase":"$Handover Cancel Phase",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Handover Cancel Phase"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "IMEI check Flag":1, "StartTime":1,fieldName:"IMEI check Flag",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "IMEI check Flag":"$IMEI check Flag",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$IMEI check Flag"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "IMEISV":1, "StartTime":1,fieldName:"IMEISV",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "IMEISV":"$IMEISV",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$IMEISV"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Init proc cause":1, "StartTime":1,fieldName:"Init proc cause",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Init proc cause":"$Init proc cause",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Init proc cause"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Internal Physical Location":1, "StartTime":1,fieldName:"Internal Physical Location",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Internal Physical Location":"$Internal Physical Location",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Internal Physical Location"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "LAC":1, "StartTime":1,fieldName:"LAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "LAC":"$LAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$LAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Message Type":1, "StartTime":1,fieldName:"Message Type",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Message Type":"$Message Type",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Message Type"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "MSISDN":1, "StartTime":1,fieldName:"MSISDN",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "MSISDN":"$MSISDN",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$MSISDN"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "New GUTI MMEC":1, "StartTime":1,fieldName:"New GUTI MMEC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "New GUTI MMEC":"$New GUTI MMEC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$New GUTI MMEC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "New GUTI MMEGI":1, "StartTime":1,fieldName:"New GUTI MMEGI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "New GUTI MMEGI":"$New GUTI MMEGI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$New GUTI MMEGI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old GUTI MMEC":1, "StartTime":1,fieldName:"Old GUTI MMEC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old GUTI MMEC":"$Old GUTI MMEC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old GUTI MMEC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old GUTI MMEGI":1, "StartTime":1,fieldName:"Old GUTI MMEGI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old GUTI MMEGI":"$Old GUTI MMEGI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old GUTI MMEGI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old LAC":1, "StartTime":1,fieldName:"Old LAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old LAC":"$Old LAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old LAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old RAC":1, "StartTime":1,fieldName:"Old RAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old RAC":"$Old RAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old RAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Old TAC":1, "StartTime":1,fieldName:"Old TAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Old TAC":"$Old TAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Old TAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDN Address in used":1, "StartTime":1,fieldName:"PDN Address in used",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDN Address in used":"$PDN Address in used",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDN Address in used"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDN Gateway Address":1, "StartTime":1,fieldName:"PDN Gateway Address",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDN Gateway Address":"$PDN Gateway Address",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDN Gateway Address"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDN Type":1, "StartTime":1,fieldName:"PDN Type",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDN Type":"$PDN Type",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDN Type"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDP address in use":1, "StartTime":1,fieldName:"PDP address in use",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDP address in use":"$PDP address in use",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDP address in use"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDP type in used":1, "StartTime":1,fieldName:"PDP type in used",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDP type in used":"$PDP type in used",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDP type in used"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "PDP type request by MS":1, "StartTime":1,fieldName:"PDP type request by MS",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "PDP type request by MS":"$PDP type request by MS",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$PDP type request by MS"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Peer SGSN Address":1, "StartTime":1,fieldName:"Peer SGSN Address",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Peer SGSN Address":"$Peer SGSN Address",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Peer SGSN Address"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "QCI":1, "StartTime":1,fieldName:"QCI",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "QCI":"$QCI",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$QCI"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "RAC":1, "StartTime":1,fieldName:"RAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "RAC":"$RAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$RAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "RNC ID":1, "StartTime":1,fieldName:"RNC ID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "RNC ID":"$RNC ID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$RNC ID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "S1-U eNodeB Address1":1, "StartTime":1,fieldName:"S1-U eNodeB Address1",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "S1-U eNodeB Address1":"$S1-U eNodeB Address1",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$S1-U eNodeB Address1"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "S1-U Serving Gateway Address1":1, "StartTime":1,fieldName:"S1-U Serving Gateway Address1",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "S1-U Serving Gateway Address1":"$S1-U Serving Gateway Address1",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$S1-U Serving Gateway Address1"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Serving Gateway Address":1, "StartTime":1,fieldName:"Serving Gateway Address",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Serving Gateway Address":"$Serving Gateway Address",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Serving Gateway Address"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "SGS RePaging Count":1, "StartTime":1,fieldName:"SGS RePaging Count",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "SGS RePaging Count":"$SGS RePaging Count",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$SGS RePaging Count"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "SGW Change Flag":1, "StartTime":1,fieldName:"SGW Change Flag",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "SGW Change Flag":"$SGW Change Flag",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$SGW Change Flag"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "SRVCC Cause":1, "StartTime":1,fieldName:"SRVCC Cause",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "SRVCC Cause":"$SRVCC Cause",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$SRVCC Cause"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "SRVCC Preparation Period":1, "StartTime":1,fieldName:"SRVCC Preparation Period",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "SRVCC Preparation Period":"$SRVCC Preparation Period",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$SRVCC Preparation Period"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "State MachineName":1, "StartTime":1,fieldName:"State MachineName",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "State MachineName":"$State MachineName",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$State MachineName"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Target RNC ID":1, "StartTime":1,fieldName:"Target RNC ID",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Target RNC ID":"$Target RNC ID",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Target RNC ID"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Target TAC":1, "StartTime":1,fieldName:"Target TAC",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Target TAC":"$Target TAC",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Target TAC"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Traffic Class":1, "StartTime":1,fieldName:"Traffic Class",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Traffic Class":"$Traffic Class",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Traffic Class"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "User Home PLMN":1, "StartTime":1,fieldName:"User Home PLMN",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "User Home PLMN":"$User Home PLMN",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$User Home PLMN"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "VLR No_":1, "StartTime":1,fieldName:"VLR No_",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "VLR No_":"$VLR No_",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$VLR No_"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();
            await tmpCollection.aggregate([
                { $project: { 
                    ChrType :1, AccessType: 1, ProtocolCause: 1, "Procedure identification": 1, "Voice domain preference and UE's usage setting":1, "StartTime":1,fieldName:"Voice domain preference and UE's usage setting",
                    year: 1, month:1, day:1,hour:1
                }},
                {$group:{
                    _id:{
                        "Voice domain preference and UE's usage setting":"$Voice domain preference and UE's usage setting",
            ChrType :"$ChrType",
            AccessType: "$AccessType",
            ProtocolCause: "$ProtocolCause", 
             ProcedureIdentification: "$Procedure identification",
            year: "$year", month: "$month", day: "$day", hour:"$hour" 
            },
            "value":{ "$first":"$Voice domain preference and UE's usage setting"},
            "fieldName":{"$first":"$fieldName"},
            count:{"$sum":1},
            "ts":{"$first":"$StartTime"}
            }
            },
            {$project:{
            _id:0,
            "value":1,
            "fieldName":1,count:1,
            withHour:{$dateToString:{format:"%Y-%m-%d %H",date:"$ts"}},
            ts:1
            }},
            {$project:{
            "value":1,
            "fieldName":1,
            count:1,
            ts:{$dateFromString:{dateString:"$withHour",format:"%Y-%m-%d %H",onError:"$ts"}},
            }},
            {$merge:{into:{
            db:finalDB,coll:collName
            },whenMatched:"keepExisting"}}
            ],{"allowDiskUse":true}).toArray();


            


            }
        }
        

        await client.close();

    }
    catch(err){
        console.error(err);
        await client.close();
    }
}

// console.log(new Date().getHours());
// console.log(new Date().getMinutes());
main()
    .then(()=>console.log(new Date().getMinutes()))
    .catch(err=>{
        console.log(new Date().getMinutes());
        console.error(err);
    })