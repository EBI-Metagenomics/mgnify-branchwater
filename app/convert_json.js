// const json_file = require('public_emg_runs.json');

const fs = require('fs');
const path = require('path');

const jsonPath = path.join(__dirname, 'public_emg_runs.json');
const jsonData = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
const outputArray = [];
// const restructedData = jsonData.map(datum => {
//     outputArray.push(datum.ACCESSION);
//     return datum.ACCESSION
// });
// const outputFilePath = path.join(__dirname, 'converted_public_emg_runs.json');
// const outputContent = `${JSON.stringify(restructedData, null, 2)};`;
// fs.writeFileSync(outputFilePath, outputContent, 'utf8');


const restructedData = { accessions: jsonData.map(datum => datum.ACCESSION) };
const outputFilePath = path.join(__dirname, 'converted_public_emg_runs.json');
const outputContent = JSON.stringify(restructedData, null, 2);
fs.writeFileSync(outputFilePath, outputContent, 'utf8');




console.log(`Reconstructed data: ${JSON.stringify(restructedData)[0]}`);
// for (const jsonDatum of jsonData) {
//     console.log(`Processing JSON datum: ${JSON.stringify(jsonDatum)}`);
// }

// const fs = require('fs');
// const path = require('path');
// const outputFilePath = path.join(__dirname, 'public_emg_runs.js');
// const jsonData = JSON.stringify(json_file, null, 2);
// for (const jsonDatum of jsonData) {
//     console.log(`Processing JSON datum: ${JSON.stringify(jsonDatum)}`);
// }