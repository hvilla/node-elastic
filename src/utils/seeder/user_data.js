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
            }else{
              auxData[name]=data[index];
            }
        });
        tempData.push(auxData);
    });
    return tempData;
}

const bulkIndex = function bulkIndex(index,type, data) {
    let bulkBody = [];
  
    data.forEach(item => {
      //console.log(item);
      bulkBody.push({
        index: {
          _index: index,
          _type: type,
          _id: item._id,
          
        }
        
      });
  
      bulkBody.push(item);
    });
  
esClient.bulk({body: bulkBody})
    .then(response => {
      let errorCount = 0;
      response.items.forEach(item => {
        if (item.index && item.index.error) {
          ++errorCount;
          //console.log(, item.index.error);
        }
      });
      console.log(
        `Successfully indexed ${data.length - errorCount}
         out of ${data.length} items`
      );
    })
    .catch(console.err);
  };


const createIndex = async function(nameIndex){
  esClient.indices.exists({index:nameIndex},(err,res,status)=>{
    if(res){
      console.log('Index already exists, proceding with data importing');
    }else{
      esClient.indices.create({index:nameIndex},(err,res,status)=>{
        esClient.indices.putMapping({
          index: nameIndex,
          includeTypeName:true,
          type: 'user',
          body: {
            properties: { 
              first_name:{type:'keyword'},
              last_name:{type:'keyword'},
              login_date:{type:'keyword'}
            }
          }
        })
      });
    }
  });
}

async function main(){
    try {
        const parsedData = await extractor();
        await createIndex(fileName);
        for (let index = 0; index < 10; index++) {
            await bulkIndex(fileName,'user', parsedData);
        }
    } catch (error) {
        console.log(error)
    }
}

main();
