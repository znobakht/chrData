import { MongoClient } from "mongodb";
import { mongoURL } from "../config/keys.js";
const dbNameTemplate = "2022120";
let totalATS = [];
let totalPIS = [];
let totalPCS = [];
let totalCTS = [];

async function main() {
  try {
    const clinet = await MongoClient.connect(mongoURL);
    console.log("connected to db");
    for (i = 5; i < 8; i++) {
      const db = clinet.db(`${dbNameTemplate}${i}`);
      const collection = db.collection("chr");
      let ATS = await collection.distinct("AccessType");
      console.log(i);
      // console.log(ATS)
      for (let i = 0; i < ATS.length; i++) {
        if (!(totalATS.indexOf(ATS[i]) + 1)) {
          totalATS.push(ATS[i]);
        }
      }
      let PIS = await collection.distinct("Procedure identification");
      for (let i = 0; i < PIS.length; i++) {
        if (!(totalPIS.indexOf(PIS[i]) + 1)) {
          totalPIS.push(PIS[i]);
        }
      }
      let PCS = await collection.distinct("ProtocolCause");
      for (let i = 0; i < PCS.length; i++) {
        if (!(totalPCS.indexOf(PCS[i]) + 1)) {
          totalPCS.push(PCS[i]);
        }
      }
      let CTS = await collection.distinct("ChrType");
      for (let i = 0; i < CTS.length; i++) {
        if (!(totalCTS.indexOf(CTS[i]) + 1)) {
          totalCTS.push(CTS[i]);
        }
      }
    }

    console.log(totalATS);
    console.log(totalPIS);
    console.log(totalPCS);
    console.log(totalCTS);

    clinet.close();
  } catch (err) {
    console.error(err);
    clinet.close();
  }
}

main()
  .then()
  .catch((err) => console.log(err));
