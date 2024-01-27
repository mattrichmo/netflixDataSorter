import fs from 'fs';
import axios from 'axios';
import cheerio from 'cheerio';
import { v4 as uuidv4 } from 'uuid';
import chalk from 'chalk';

// Function to get the last scraped index from the JSONL file
const getLastScrapedIndex = (filePath, netflixData) => {
    try {
        if (!fs.existsSync(filePath)) {
            return 0; // If the file doesn't exist, start from the beginning
        }

        const lines = fs.readFileSync(filePath, 'utf-8').trim().split('\n');
        if (lines.length > 0) {
            const lastLine = JSON.parse(lines[lines.length - 1]);
            if (lastLine && lastLine.netflixData && lastLine.netflixData.rawTitle) {
                const lastScrapedRawTitle = lastLine.netflixData.rawTitle;

                // Find the index of the last scraped rawTitle in the netflixData array
                const index = netflixData.findIndex(item => item.netflixData.rawTitle === lastScrapedRawTitle);

                return index !== -1 ? index : 0; // If not found, start from the beginning
            }
        }
    } catch (error) {
        console.error('Error getting last scraped index:', error);
    }
    return 0;
};

// Function to get IMDb info and set it under each contentData item
const getIMDBinfo = async (query, index, lastScrapedIndex) => {
    try {
        if (index <= lastScrapedIndex) {
            console.log(chalk.gray(`Skipping row ${index + 1} as it was already scraped.`));
            return null;
        }

        console.log(chalk.dim(`\nFetching IMDb info for row ${index + 1} - Query:`), chalk.cyan(`${query}`));

        const headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        };

        let retryCount = 0;
        let imdbInfo = null;

        while (retryCount < 2) {
            try {
                // Send a query to IMDb using Cheerio
                const imdbPage = await axios.get(`https://www.imdb.com/find/?q=${query}`, { headers, timeout: 10000 });
                const $ = cheerio.load(imdbPage.data);

                // Select the first result from the array under the "results" selector
                const firstResult = $('[data-testid="find-results-section-title"] div.sc-17bafbdb-2 li.ipc-metadata-list-summary-item').first();

                // Extracting information based on provided selectors
                const imdbLink = `https://www.imdb.com${firstResult.find('a').attr('href').split('?')[0]}`;
                const title = firstResult.find('a').text();
                const type = firstResult.find('li:nth-of-type(2) span').text();
                const releaseDate = firstResult.find('.ipc-metadata-list-summary-item__tl li:nth-of-type(1) span').text();

                // Extracting stars as an array of objects (splitting by comma)
                const starNames = firstResult.find('.ipc-metadata-list-summary-item__stl span').text();
                const stars = starNames.split(',').map(star => ({ starName: star.trim() }));

                // Creating the IMDb info object
                imdbInfo = {
                    imdbLink,
                    title,
                    type,
                    releaseDate,
                    stars,
                };
                console.log(chalk.dim(`Title:`), chalk.cyan(title), chalk.dim(`Type:`), chalk.blue(type), chalk.dim(`Release Date:`), chalk.green(releaseDate), chalk.dim(`Stars:`), chalk.magenta(starNames));
                console.log(`\n`)
                break; // Break out of the retry loop if successful
            } catch (error) {
                console.error(chalk.yellow(`Retry ${retryCount + 1} failed for row ${index + 1} - Query: ${query}`), error);
                retryCount++;
            }
        }

        if (!imdbInfo) {
            console.error(chalk.red(`Error fetching IMDb info for row ${index + 1} - Query: ${query}`));

            // Save the error information to an error JSONL file
            fs.appendFileSync('errorData.jsonl', JSON.stringify({ error: 'Max retries reached', query, index }) + '\n');
        }

        return imdbInfo;
    } catch (error) {
        console.error(chalk.red(`Error fetching IMDb info for row ${index + 1} - Query: ${query}`), error);

        // Save the error information to an error JSONL file
        fs.appendFileSync('errorData.jsonl', JSON.stringify({ error, query, index }) + '\n');

        return null;
    }
};


// Function to process Netflix data and make IMDb API calls sequentially
const processNetflixData = async () => {
    try {
        const filePath = 'data/cleaned/netflixCleanedData.json';
        const outputFilePath = 'data/cleaned/imdbData.jsonl';
        const errorFilePath = 'errorData.jsonl';

        // Read the last scraped index from the JSONL file
        const rawData = fs.readFileSync(filePath);
        const netflixData = JSON.parse(rawData);
        const lastScrapedIndex = getLastScrapedIndex(outputFilePath, netflixData);

        let currentIndex = lastScrapedIndex;

        // Create a set to store unique items based on the cleanTitle
        const uniqueItemsSet = new Set();

        while (currentIndex < netflixData.length) {
            const netflixItem = netflixData[currentIndex].netflixData;
            const cleanTitle = netflixItem.cleanTitle.split('//')[0].trim();

            // Check if the cleanTitle is already in the set (indicating a duplicate)
            if (!uniqueItemsSet.has(cleanTitle)) {
                uniqueItemsSet.add(cleanTitle);

                // Get IMDb info for the current item
                const imdbInfo = await getIMDBinfo(cleanTitle, currentIndex, lastScrapedIndex);

                if (imdbInfo) {
                    // Add indexNum and titleUUID to the netflixItem
                    netflixData[currentIndex].netflixData.indexNum = currentIndex + 1;
                    netflixData[currentIndex].netflixData.titleUUID = uuidv4();

                    netflixData[currentIndex].netflixData.imdbInfo = imdbInfo;

                    // Append the item to the JSONL file
                    fs.appendFileSync(outputFilePath, JSON.stringify({ netflixData: netflixData[currentIndex].netflixData }) + '\n');
                }
            } else {
                console.log(chalk.gray(`Skipping duplicate item for row ${currentIndex + 1} with cleanTitle: ${cleanTitle}`));
            }

            // Move to the next item
            currentIndex++;

            // Wait for a moment before processing the next item
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    } catch (error) {
        console.error(chalk.red('Error processing Netflix data:'), error);
    }
};

// Call the function to process Netflix data and make IMDb API calls sequentially
processNetflixData();


