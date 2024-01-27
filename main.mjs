import fs from 'fs';
import axios from 'axios';
import cheerio from 'cheerio';
import chalk from 'chalk';

// Function to read the last scraped index from the manifest
const getLastScrapedIndex = (filePath) => {
    const scrapedManifestFilePath = 'scrapedManifest.jsonl';

    try {
        if (!fs.existsSync(scrapedManifestFilePath)) {
            return 0;
        }

        const manifestLines = fs.readFileSync(scrapedManifestFilePath, 'utf-8').split('\n').filter(line => line.trim() !== '');

        if (manifestLines.length === 0) {
            return 0;
        }

        const lastManifestLine = JSON.parse(manifestLines[manifestLines.length - 1]);
        return lastManifestLine.index;
    } catch (error) {
        console.error(chalk.red(`Error reading last scraped index from manifest`), error);
        return -1;
    }
};

// Function to get IMDb info for a given query, index, and last scraped index
const getIMDBinfo = async (query, index, lastScrapedIndex, data) => {
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
                const imdbPage = await axios.get(`https://www.imdb.com/find/?q=${query}`, { headers, timeout: 10000 });
                const $ = cheerio.load(imdbPage.data);

                // Extract IMDb information from the page
                const results = $('[data-testid="find-results-section-title"] div.sc-17bafbdb-2 li.ipc-metadata-list-summary-item');
                const validResult = results.toArray().find(result => {
                    const type = $(result).find('li:nth-of-type(2) span').text();
                    return !type.includes('Podcast');
                });

                if (validResult) {
                    // Extract IMDb information from the valid result
                    const imdbLink = `https://www.imdb.com${$(validResult).find('a').attr('href').split('?')[0]}`;
                    const title = $(validResult).find('a').text();
                    const type = $(validResult).find('li:nth-of-type(2) span').text();
                    const releaseDate = $(validResult).find('.ipc-metadata-list-summary-item__tl li:nth-of-type(1) span').text();
                    const starNames = $(validResult).find('.ipc-metadata-list-summary-item__stl span').text();
                    const stars = starNames.split(',').map(star => ({ starName: star.trim() }));

                    imdbInfo = {
                        imdbLink,
                        title,
                        type,
                        releaseDate,
                        stars,
                    };

                    // Log IMDb information including the returned type
                    console.log(chalk.dim(`Title:`), chalk.cyan(title), chalk.dim(`Type:`), chalk.blue(type), chalk.dim(`Release Date:`), chalk.green(releaseDate), chalk.dim(`Stars:`), chalk.magenta(starNames));
                    console.log(`Returned Type: ${type}\n`);
                } else {
                    const firstResult = results.first();
                    const firstType = $(firstResult).find('li:nth-of-type(2) span').text();
                    console.log(chalk.red(`No valid result found excluding 'Podcast'.`));
                    console.log(`First Result Type: ${firstType}\n`);
                    const errorObject = {
                        error: 'No valid result found excluding "Podcast"',
                        query,
                        index,
                        originalData: JSON.parse(data),
                    };
                    fs.appendFileSync('errorData.jsonl', JSON.stringify(errorObject) + '\n');
                    return null; // Return null to signify no valid result
                }

                break;
            } catch (error) {
                // Retry on failure
                console.error(chalk.yellow(`Retry ${retryCount + 1} failed for row ${index + 1} - Query: ${query}`), error);
                retryCount++;
            }
        }

        if (!imdbInfo) {
            // Log error if no valid result is found
            console.error(chalk.red(`Error fetching IMDb info for row ${index + 1} - Query: ${query}`));

            const errorObject = {
                error: 'Max retries reached or no valid result found',
                query,
                index,
                originalData: JSON.parse(data),
            };
            fs.appendFileSync('errorData.jsonl', JSON.stringify(errorObject) + '\n');
        }

        return imdbInfo;
    } catch (error) {
        // Log error if there's an exception
        console.error(chalk.red(`Error fetching IMDb info for row ${index + 1} - Query: ${query}`), error);

        const errorObject = {
            error,
            query,
            index,
            originalData: JSON.parse(data),
        };
        fs.appendFileSync('errorData.jsonl', JSON.stringify(errorObject) + '\n');
        return null;
    }
};

// Function to process Netflix data
const processNetflixData = async () => {
    try {
        // File paths
        const cleanedDataFilePath = 'data/cleaned/netflixCleanedData.jsonl';
        const imdbDataFilePath = 'data/cleaned/imdbData.jsonl';
        const errorFilePath = 'errorData.jsonl';
        const scrapedManifestFilePath = 'scrapedManifest.jsonl';

        // Read the last scraped index from the manifest
        const lastScrapedIndex = getLastScrapedIndex(imdbDataFilePath);

        // Read cleaned data from the file
        const cleanedDataLines = fs.existsSync(cleanedDataFilePath) ? fs.readFileSync(cleanedDataFilePath, 'utf-8').split('\n') : [];
        const cleanedDataObjects = cleanedDataLines.map(line => line.trim()).filter(Boolean);

        // Create write streams for IMDb data, error data, and scraped manifest
        const imdbDataStream = fs.createWriteStream(imdbDataFilePath, { flags: 'a' });
        const errorDataStream = fs.createWriteStream(errorFilePath, { flags: 'a' });
        const scrapedManifestStream = fs.createWriteStream(scrapedManifestFilePath, { flags: 'a' });

        // Array to store IMDb information for each processed row
        const imdbInfoArray = [];

        // Batch processing variables
        const batchSize = 10;
        let batchCount = 0;
        let successfulCount = 0;
        let erroredCount = 0;
        let timedOutCount = 0;

        // Process each row in the cleaned data
        for (let i = 0; i < cleanedDataObjects.length; i += batchSize) {
            // Initialize batch arrays
            const batchData = cleanedDataObjects.slice(i, i + batchSize);
            const batchPromises = [];

            // Process each item in the batch concurrently
            for (let j = 0; j < batchData.length; j++) {
                const data = batchData[j];

                // Ensure the line is not empty before parsing
                if (data !== '') {
                    const promise = (async () => {
                        try {
                            const jsonData = JSON.parse(data);
                            const imdbQuery = encodeURIComponent(jsonData.netflixData.searchTerm.replace(/\s+/g, ' '));
                            const imdbInfo = await getIMDBinfo(imdbQuery, i + j, lastScrapedIndex, data);

                            // Store IMDb information in the array
                            if (imdbInfo) {
                                imdbInfoArray.push({ imdbInfo, searchTerm: jsonData.netflixData.searchTerm, index: i + j });

                                // Write IMDb information to the IMDb data file and scraped manifest
                                imdbDataStream.write(JSON.stringify({ imdbInfo, searchTerm: jsonData.netflixData.searchTerm, index: i + j }) + '\n');
                                scrapedManifestStream.write(JSON.stringify({ searchTerm: jsonData.netflixData.searchTerm, index: i + j }) + '\n');

                                // Log processed row
                                console.log(`Processed row ${i + j + 1} - IMDb info:`, imdbInfo);

                                // Increment successful count
                                successfulCount++;
                            }
                        } catch (parseError) {
                            // Log error if there's an issue parsing the JSON
                            console.error(chalk.red(`Error parsing JSON for row ${i + j + 1}`), parseError);

                            // Increment errored count
                            erroredCount++;
                        }
                    })();

                    batchPromises.push(promise);
                } else {
                    // Log a warning if the line is empty
                    console.warn(chalk.yellow(`Warning: Empty line found at row ${i + j + 1}`));

                    // Increment errored count for empty line
                    erroredCount++;
                }
            }

            // Wait for all promises in the batch to complete before moving to the next batch
            await Promise.allSettled(batchPromises);

            // Log batch summary
            batchCount++;
            console.log(`Batch ${batchCount} Summary - Successful: ${successfulCount}, Errored: ${erroredCount}, Timed Out: ${timedOutCount}`);

            // Reset counts for the next batch
            successfulCount = 0;
            erroredCount = 0;
            timedOutCount = 0;
        }

        // Close write streams
        imdbDataStream.end();
        errorDataStream.end();
        scrapedManifestStream.end();

        console.log('Processing complete.');
    } catch (error) {
        // Log error if there's an exception during processing
        console.error(chalk.red('Error processing Netflix data and making IMDb API calls'), error);
    }
};

// Run the processNetflixData function
processNetflixData();

