import fs from 'fs';
import csv from 'csv-parser';
import { v4 as uuidv4 } from 'uuid';

// Function to parse CSV row into JSON object
const parseCSVRow = (row, index) => {
    const title = row.Title || ''; // Ensure that title is not undefined
    const availableGlobally = row['Available Globally?'] || '';
    const hoursViewed = parseFloat(row['Hours Viewed'].replace(/,/g, '')) || 0;

    // Extracting searchTerm
    const searchTerm = title.split('//')[0].trim();

    // Extracting release date
    const releaseDate = row['Release Date'] || '';

    // Creating itemUUID using uuid v4
    const itemUUID = uuidv4();

    const coreTitle = title.split(':')[0].trim();
    const availableGloballyBool = availableGlobally.toLowerCase() === 'yes';

    const newObj = {
        netflixData: {
            rawTitle: title,
            coreTitle: coreTitle,
            availableGlobally: availableGloballyBool,
            releaseDate: releaseDate,
            hoursViewed: hoursViewed,
            searchTerm: searchTerm,
            itemUUID: itemUUID,
            indexNum: index, // Add indexNum property
            relationships: {
                siblings: [],
            },
        },
    };

    return newObj;
};

// Function to establish relationships between items with matching coreTitle
const createRelationships = (allContentData) => {
    allContentData.forEach((item, index) => {
        const siblings = allContentData
            .filter((sibling) => sibling.netflixData.coreTitle === item.netflixData.coreTitle && sibling !== item)
            .map((sibling) => ({
                itemUUID: sibling.netflixData.itemUUID,
                coreTitle: sibling.netflixData.coreTitle,
                title: sibling.netflixData.rawTitle,
                searchTitle: sibling.netflixData.searchTerm,
            }));

        item.netflixData.relationships.siblings = siblings;
    });
};

const results = [];

// Read CSV file and parse each row
fs.createReadStream('./data/raw/netflixData.csv')
    .pipe(csv())
    .on('data', (data, index) => {
        console.log(`data: ${JSON.stringify(data)}`);

        const contentData = parseCSVRow(data, index);
        results.push(contentData);
    })
    .on('end', () => {
        // Establish relationships between items with matching coreTitle
        createRelationships(results);

        // Stream the JSON Lines data to a new file
        const jsonlStream = fs.createWriteStream('./data/cleaned/netflixCleanedData.jsonl');
        results.forEach((item) => {
            jsonlStream.write(JSON.stringify(item) + '\n');
        });
        jsonlStream.end();

        console.log('Processing complete.');
    });
