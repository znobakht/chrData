const db = require("./db");
const { allKeys } = require("./allkeys");
// const query = require("./query");
// const query28 = require("./query_day28");
const fs = require("fs");
async function start() {
  let startTime = new Date();
  let conn = await db.init();
  for (let index = 0; index < allKeys.length; index++) {
    try {
      const key = allKeys[index];
      console.log(`create cache started, key: ${key}`);
      let query = fs.readFileSync("./query.txt").toString();
      let query28 = fs.readFileSync("./query_day28.txt").toString();

      let days = ["20221007", "20221008", "20221009"];
      let queries = days.map(async (day) => {
        let currQuery = query;
        // if (day == "20220628") {
        //   currQuery = query28;
        // }
        currQuery = currQuery.toString().replace(/TARGET_FILED/g, key);
        currQuery = currQuery
          .toString()
          .replace(/COLL_NAME/g, key.replace(/\./g, ""));

        let currDB = conn.db(day);
        console.log(`running query on day: ${day}, key: ${key}`);
        let t1 = new Date();
        let chrColl = currDB.collection("chr");
        await chrColl
          .aggregate(JSON.parse(currQuery), { allowDiskUse: true })
          .toArray();
        let t2 = new Date();
        console.log(
          `running query on day: ${day}, key: ${key} done, time: ${
            (t2.getTime() - t1.getTime()) / 1000
          } seconds`
        );
        fs.appendFileSync(
          "keys_done.txt",
          `${key} ${day}, time: ${
            (t2.getTime() - t1.getTime()) / 1000
          } seconds\n`
        );
      });
      await Promise.all(queries);
    } catch (error) {
      console.log("error", error);
    }
  }
  await db.disconnect();
  let endTime = new Date();
  console.log(
    `All done, total time: ${
      (endTime.getTime() - startTime.getTime()) / 1000
    } seconds`
  );
}

start();
