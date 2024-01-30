// step2.mjs

import fs from 'fs';
import axios from 'axios';
import cheerio from 'cheerio';
import chalk from 'chalk';

// Function to read the last scraped index from the manifest
const getLastScrapedIndex = (filePath) => {
    const scrapedManifestFilePath = 'pass2manifest.jsonl';

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

const getIMDBData = async (imdbLink, index, lastScrapedIndex, scrapedIndicesSet) => {
    try {
        if (index <= lastScrapedIndex || scrapedIndicesSet.has(index)) {
            console.log(chalk.gray(`Skipping row ${index + 1} as it was already scraped.`));
            return null;
        }

        console.log(chalk.dim(`\nFetching IMDb data for row ${index + 1} - IMDb Link:`), chalk.cyan(`${imdbLink}`));

        const headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        };

        let retryCount = 0;
        let imdbData = null;

        while (retryCount < 2) {
            try {
                const imdbPage = await axios.get(imdbLink, { headers, timeout: 10000 });
                const $ = cheerio.load(imdbPage.data);

                // Extract IMDb data using the provided selectors
                const title = $('span.hero__primary-text').text();
                const imdbRatingElements = $('.sc-69e49b85-1 div.sc-bde20123-0');
                const ratings = imdbRatingElements.map((i, el) => {
                    const ratingElement = $(el);
                    const rating = ratingElement.find('span.sc-bde20123-1').text();
                    const numRatings = ratingElement.find('div.sc-bde20123-3').text();
                    const outOf = ratingElement.find('span:nth-of-type(2)').text();
                    return { rating, numRatings, outOf };
                }).get();

                const length = $('.sc-d8941411-2 li:nth-of-type(4)').text();
                const dates = $('.sc-d8941411-2 li:nth-of-type(2) a').text();
                const showRating = $('.sc-d8941411-2 li:nth-of-type(3) a').text();
                const cover = $('.ipc-media--poster-l img').attr('src');

                // Update selectors for storyline, genres, and keywords
                const storyline = $('.sc-9eebdf80-1 div').text().trim();

                // New genre selector implementation
                let genres = [];
                $('.ipc-chip-list--baseAlt div.ipc-chip-list__scroller span').each((i, element) => {
                    genres.push($(element).text().trim());
                });

                imdbData = {
                    title,
                    ratings,
                    length,
                    dates,
                    showRating,
                    cover,
                    storyline,
                    genres,
                };

                // Log IMDb data
                console.log(chalk.dim(`Title:`), chalk.cyan(title));
                // ... (log other fields)
                console.log(`IMDb Data Extracted\n`);

                // Add the index to the set of scraped indices
                scrapedIndicesSet.add(index);

                break;
            } catch (error) {
                // Retry on failure
                console.error(chalk.yellow(`Retry ${retryCount + 1} failed for row ${index + 1} - IMDb Link: ${imdbLink}`), error);
                retryCount++;
            }
        }

        if (!imdbData) {
            // Log error if no valid result is found
            console.error(chalk.red(`Error fetching IMDb data for row ${index + 1} - IMDb Link: ${imdbLink}`));
            return null; // Return null to signify no valid result
        }

        return { imdbData, index };
    } catch (error) {
        // Log error if there's an exception
        console.error(chalk.red(`Error fetching IMDb data for row ${index + 1} - IMDb Link: ${imdbLink}`), error);
        return null;
    }
};



const processIMDBDataFile = async () => {
    try {
        // File paths
        const imdbDataFilePath = 'data/cleaned/imdbData.jsonl';
        const imdbDataFinalFilePath = 'IMDB-DATA-FINAL.jsonl';
        const errorFilePath = 'errorData_pass2.jsonl';
        const scrapedManifestFilePath = 'pass2manifest.jsonl';

        // Read the last scraped index from the manifest
        const lastScrapedIndex = getLastScrapedIndex(imdbDataFinalFilePath);

        // Read imdbData.jsonl
        const imdbDataLines = fs.existsSync(imdbDataFilePath) ? fs.readFileSync(imdbDataFilePath, 'utf-8').split('\n') : [];
        const imdbDataObjects = imdbDataLines.map(line => line.trim()).filter(Boolean);

        // Create write streams for IMDb final data, error data, and scraped manifest
        const imdbDataFinalStream = fs.createWriteStream(imdbDataFinalFilePath, { flags: 'a' });
        const errorDataStream = fs.createWriteStream(errorFilePath, { flags: 'a' });
        const scrapedManifestStream = fs.createWriteStream(scrapedManifestFilePath, { flags: 'a' });

        // Set to store scraped indices
        const scrapedIndicesSet = new Set();

        // Batch processing variables
        const batchSize = 10;

        // Process each batch of IMDb objects
        for (let batchStart = 0; batchStart < imdbDataObjects.length; batchStart += batchSize) {
            const batchPromises = imdbDataObjects.slice(batchStart, batchStart + batchSize).map((data, batchIndex) => {
                const index = batchStart + batchIndex;
                const imdbDataObject = JSON.parse(data);
                const imdbLink = imdbDataObject.imdbData.imdbLink;

                if (imdbLink.trim() !== '') {
                    return getIMDBData(imdbLink, index, lastScrapedIndex, scrapedIndicesSet)
                        .then(imdbData => {
                            if (imdbData) {
                                imdbDataObject.newIMDBData = imdbData.imdbData;
                                imdbDataFinalStream.write(JSON.stringify(imdbDataObject) + '\n');
                                scrapedManifestStream.write(JSON.stringify({ index }) + '\n');
                                console.log(`Processed row ${index + 1} - IMDb Data:`, imdbDataObject);
                            }
                        })
                        .catch(error => {
                            errorDataStream.write(JSON.stringify({ index, error: 'Error fetching data' }) + '\n');
                            console.error(chalk.red(`Error processing row ${index + 1}:`), error);
                        });
                }
            });

            // Wait for all promises in the batch to complete
            await Promise.allSettled(batchPromises);
        }

        // Close write streams
        imdbDataFinalStream.end();
        errorDataStream.end();
        scrapedManifestStream.end();

        console.log('Processing complete.');
    } catch (error) {
        console.error(chalk.red('Error processing imdbData.jsonl and making IMDb API calls'), error);
    }
};

// Run the processIMDBDataFile function
processIMDBDataFile();
