import fs from 'fs';
import csv from 'csv-parser';
import { v4 as uuidv4 } from 'uuid';

// Function to organize objects based on coreTitle
const organizeObjects = (parsedObjects) => {
    const organizedObjects = [];

    parsedObjects.forEach((item) => {
        const existingItem = organizedObjects.find((existing) => existing.coreTitle === item.netflixData.coreTitle);

        if (existingItem) {
            // Matching coreTitle found, add the item as a child
            existingItem.relationships.children.push(item.netflixData);
        } else {
            // No matching coreTitle, add the item as a top-level object
            organizedObjects.push({
                coreTitle: item.netflixData.coreTitle,
                imdbData: null,
                meta: {
                    numItem: 0, // Rename numSeason to numItem
                    totalHoursViewed: 0,
                },
                relationships: {
                    children: [item.netflixData],
                },
            });
        }
    });

    return organizedObjects;
};

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

        // Organize objects based on coreTitle
        const organizedResults = organizeObjects(results);

        // Calculate total hours viewed and numItem for each organized object
        organizedResults.forEach((item) => {
            item.meta.totalHoursViewed = item.relationships.children.reduce((total, child) => total + child.hoursViewed, 0);
            item.meta.numItem = item.relationships.children.length;
        });

        // Stream the JSON Lines data to a new file (organized data)
        const organizedJsonlStream = fs.createWriteStream('./data/cleaned/organizedNetflixData.jsonl');
        organizedResults.forEach((item) => {
            organizedJsonlStream.write(JSON.stringify(item) + '\n');
        });
        organizedJsonlStream.end();

        // Stream the JSON Lines data to another file (cleaned data)
        const cleanedJsonlStream = fs.createWriteStream('./data/cleaned/netflixCleanedData.jsonl');
        results.forEach((item) => {
            cleanedJsonlStream.write(JSON.stringify(item) + '\n');
        });
        cleanedJsonlStream.end();

        console.log('Processing complete.');
    });
