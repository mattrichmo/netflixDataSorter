import fs from 'fs';
import csv from 'csv-parser';
import { v4 as uuidv4 } from 'uuid';
import path from 'path';

// Helper functions for title processing
const cleanTitle = (title) => {
    // Logic to clean the title
    return title.split('//')[0].split(/:\sseason\s\d+|:\spart\s\d+/i)[0].trim();
};

const extractCoreTitle = (title) => {
    // Define a regular expression that matches season or part indicators
    const seasonPartRegex = /:\s*(season\s+\d+|part\s+\d+|\d+\s*-\s*\d+)/i;

    // Check if the title matches the pattern
    if (seasonPartRegex.test(title)) {
        return title.split(seasonPartRegex)[0].trim();
    }

    // Split the title by '//' and keep the first part
    const parts = title.split('//');
    let coreTitle = parts[0].trim();

    // Remove phrases like "Limited Series"
    coreTitle = coreTitle.replace(/\bLimited\s+Series\b/g, '');

    // Remove numbers inside parentheses and the parentheses themselves
    coreTitle = coreTitle.replace(/\([^)]*\)/g, '');

    // Remove foreign language characters using a regex pattern
    coreTitle = coreTitle.replace(/[^A-Za-z0-9\s]/g, '');

    return coreTitle;
};





const extractSecondaryLanguageTitle = (title) => {
    // Logic to extract secondary language title
    const secondaryTitleMatch = title.match(/\/\/\s*(.+)/);
    if (secondaryTitleMatch) {
        return secondaryTitleMatch[1].trim();
    }
    return null;
};

// Function to extract season number from title
const extractSeasonNumber = (title) => {
    const seasonMatch = title.match(/Season (\d+)/i);
    return seasonMatch ? parseInt(seasonMatch[1], 10) : 0; // Default to 0 if no season number is found
};

// Function to parse CSV row into JSON object
const parseCSVRow = (row) => {
    const coreTitle = extractCoreTitle(row.Title || '');
    const secondaryLanguageTitle = extractSecondaryLanguageTitle(row.Title || ''); // Extract secondary language title
    const hoursWatched = parseFloat(row['Hours Viewed'].replace(/,/g, '')) || 0;
    const availableGlobally = row['Available Globally?'].toLowerCase() === 'yes';
    const releaseDate = row['Release Date'] || '';

    return {
        meta: {
            contentUUID: uuidv4(),
            coreTitle: coreTitle, // Only coreTitle is retained
            secondaryLanguageTitle: secondaryLanguageTitle, // Secondary language title
        },
        data: {
            totals: {
                seasonsInNetflixData: 0, // To be computed
                totalHoursWatched: hoursWatched,
            },
            relationships: [{
                itemUUID: uuidv4(),
                title: row.Title || '',
                availableGlobally: availableGlobally,
                hoursWatched: hoursWatched,
                releaseDate: releaseDate,
            }],
            sources: {
                // Logic to populate sources if required
            }
        }
    };
};

// Function to establish relationships between items with matching coreTitle
const createRelationships = (allContentData) => {
    allContentData.forEach((item) => {
        // Ensure each item has a self-referential relationship
        if (item.data.relationships.length === 0) {
            item.data.relationships.push({
                itemUUID: item.meta.contentUUID,
                title: item.meta.coreTitle, // Use coreTitle
                availableGlobally: false, // Set default or extract from row
                hoursWatched: item.data.totals.totalHoursWatched,
                releaseDate: '', // Set default or extract from row
            });
        }

        // Add relationships to siblings
        const siblings = allContentData
            .filter((sibling) => sibling.meta.coreTitle === item.meta.coreTitle && sibling.meta.contentUUID !== item.meta.contentUUID)
            .map((sibling) => ({
                itemUUID: sibling.meta.contentUUID,
                title: sibling.meta.coreTitle, // Use coreTitle
                availableGlobally: sibling.data.relationships[0].availableGlobally,
                hoursWatched: sibling.data.relationships[0].hoursWatched,
                releaseDate: sibling.data.relationships[0].releaseDate,
            }));

        item.data.relationships = item.data.relationships.concat(siblings);
    });
};

// Function to organize objects based on coreTitle
const organizeObjects = (parsedObjects) => {
    const organizedObjects = {};

    parsedObjects.forEach((item) => {
        const coreTitle = item.meta.coreTitle;

        if (!organizedObjects[coreTitle]) {
            organizedObjects[coreTitle] = {...item, data: {...item.data, relationships: []}};
        }

        organizedObjects[coreTitle].data.relationships.push(...item.data.relationships);
    });

    // Calculate total hours viewed and count seasons/items
    Object.values(organizedObjects).forEach(item => {
        // Sort the relationships array by season number
        item.data.relationships.sort((a, b) => 
            extractSeasonNumber(a.title) - extractSeasonNumber(b.title)
        );
        item.data.totals.totalHoursWatched = item.data.relationships.reduce((total, relation) => total + relation.hoursWatched, 0);
        item.data.totals.seasonsInNetflixData = item.data.relationships.length;
    });

    return Object.values(organizedObjects);
};

// Function to load CSV data, process, and organize
const cleanAndSortData = async () => {
    const results = [];

    return new Promise((resolve, reject) => {
        fs.createReadStream('../data/raw/netflixData.csv')
            .pipe(csv())
            .on('data', (data) => {
                results.push(parseCSVRow(data));
            })
            .on('end', () => {
                createRelationships(results);
                const organizedResults = organizeObjects(results);
                resolve(organizedResults);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
};

// Function to save data objects to a JSONL file
const saveSortedDataToJsonl = async (dataObjects) => {
    return new Promise((resolve, reject) => {
        const dirPath = '../data/processed';
        const filePath = path.join(dirPath, 'sortedData.jsonl');

        // Check if the directory exists, if not create it
        if (!fs.existsSync(dirPath)){
            fs.mkdirSync(dirPath, { recursive: true });
        }

        const fileStream = fs.createWriteStream(filePath);
        dataObjects.forEach(dataObject => {
            fileStream.write(JSON.stringify(dataObject) + '\n');
        });

        fileStream.end();
        fileStream.on('finish', () => resolve());
        fileStream.on('error', (error) => reject(error));
    });
};

// Main function to orchestrate the data processing
export const loadCsvData = async () => {
    try {
        const allDataObjects = await cleanAndSortData();
        await saveSortedDataToJsonl(allDataObjects);
    } catch (error) {
        console.error('Error in processing:', error);
    }
};
loadCsvData();
