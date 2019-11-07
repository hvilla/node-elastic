const path = require('path');
const fs = require('fs');
const esClient = require('../../config/elastic-client');

const folderDir = path.join(__dirname,'files');
const fileName = 'cups';

let headers = ['code','description','type','status'];

const extractor = async () => {
    let tempData = [];

    var csv_data = fs.readFileSync(`${folderDir}/${fileName}.csv`)
        .toString() // convert Buffer to string
        .replace('"','')
        .split('\n') // split string to lines
        .map(e => e.trim()) // remove white spaces for each line
        .map(e => e.split(',').map(e => e.trim())); // split each line to array

    csv_data.map(data=>{
        let auxData = {};
        headers.forEach((name,index) => {
            auxData[name]=data[index];
        });
        tempData.push(auxData);
    });
    return tempData;
}



async function createIndex(){
    try {
        return await esClient.indices.create({
            index: fileName
        });
    } catch (error) {
        console.log(error);
    }
}

const insertDoc = async function(data){
    return await esClient.index({
        index: fileName,
        body: data
    });
}

async function main(){
    try {
        const parsedData = await extractor();
        insertDoc(parsedData);
    } catch (error) {
        console.log(error)
    }
}

main();
