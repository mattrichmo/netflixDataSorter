import fs from 'fs';
import csv from 'csv-parser';
import axios from 'axios';

let allContentData = [];

// Function to parse CSV row into JSON object
const parseCSVRow = (row) => {
    const title = row.Title;
    const availableGlobally = row['Available Globally?'];
    const hoursViewed = parseFloat(row['Hours Viewed'].replace(/,/g, ''));

    const cleanTitle = title.split(':')[0].trim();
    const availableGloballyBool = availableGlobally.toLowerCase() === 'yes';

    const newObj = {
        netflixData: {
            rawTitle: title,
            cleanTitle: cleanTitle,
            availableGlobally: availableGloballyBool,
            releaseDate: '', // You can add logic to extract the release date if needed
            hoursViewed: hoursViewed,
            imdbInfo: null, // Initialize with null
        },
    };

    allContentData.push(newObj);

    return newObj;
};

// Function to get IMDb info and set it under each contentData item
const getIMDBinfo = async (query, index) => {
    try {
        console.log(`Fetching IMDb info for row ${index + 1} - Query: ${query}`);
        const response = await axios.get(`https://imdb-api.projects.thetuhin.com/search?query=${query}`);
        const imdbResult = response.data.results[0];

        if (imdbResult) {
            console.log(imdbResult); // Log the IMDb result

            return {
                id: imdbResult.id,
                title: imdbResult.title,
                year: imdbResult.year,
                type: imdbResult.type,
                image: imdbResult.image,
                imdbLink: imdbResult.imdb,
            };
        } else {
            return null;
        }
    } catch (error) {
        console.error(`Error fetching IMDb info for row ${index + 1} - Query: ${query}`);
        return null;
    }
};

const results = [];

// Read CSV file and parse each row
fs.createReadStream('./data/raw/netflixData.csv')
    .pipe(csv())
    .on('data', async (data, index) => {
        results.push(data);

        const contentData = parseCSVRow(data);

        // Use clean title as the query with proper spacing
        const imdbQuery = encodeURIComponent(contentData.netflixData.cleanTitle.replace(/\s+/g, ' '));

        // Get IMDb info and set it under contentData
        const imdbInfo = await getIMDBinfo(imdbQuery, index);
        contentData.netflixData.imdbInfo = imdbInfo;

        // Save the array of objects with clean titles and IMDb info to a new JSON file
        fs.writeFileSync('./data/cleaned/netflixCleanedData.json', JSON.stringify(allContentData, null, 2));

        console.log(`Cleaned data with IMDb info saved to netflixCleanedData.json - Row ${index + 1}`);
    })
    .on('end', () => {
        console.log('Processing complete.');
    });
