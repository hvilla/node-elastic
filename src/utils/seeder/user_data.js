const path = require('path');
const fs = require('fs');
const csv = require('async-csv');
const esClient = require('../../config/elastic-client');
const moment = require('moment');
const folderDir = path.join(__dirname,'files');
const fileName = 'user_data';

let headers = ['_id','first_name','last_name','login_date'];

const extractor = async () => {
    let tempData = [];

    var csv_data = await csv.parse(await fs.readFileSync(`${folderDir}/${fileName}.csv`),{delimiter:','});

    csv_data.map(data=>{
        let auxData = {};
        headers.forEach((name,index) => {
            if(data[index] === "true"){
                auxData[name]=true;
            }else if(data[index] === "false"){
                auxData[name]=false;
            }else if(name==='login_date'){
                auxData[name]=moment(data[index],'YYYY-MM-DD');
            }
        });
        tempData.push(auxData);
    });
    return tempData;
}

const bulkIndex = function bulkIndex(index,type, data) {
    let bulkBody = [];
  
    data.forEach(item => {
      bulkBody.push({
        index: {
          _index: index,
          _type: type,
          _id: item.id
        }
      });
  
      bulkBody.push(item);
    });
  
esClient.bulk({body: bulkBody})
    .then(response => {
      let errorCount = 0;
      response.items.forEach(item => {
        if (item.index && item.index.error) {
          console.log(++errorCount, item.index.error);
        }
      });
      console.log(
        `Successfully indexed ${data.length - errorCount}
         out of ${data.length} items`
      );
    })
    .catch(console.err);
  };


const insertDoc = async function(data){
    console.log(data);
    return await esClient.index({
        index: fileName,
        body: data
    });
}

async function main(){
    try {
        const parsedData = await extractor();
        for (let index = 0; index < 10; index++) {
            bulkIndex('test','med_spec', parsedData);
        }
    } catch (error) {
        //console.log(error)
    }
}

main();
