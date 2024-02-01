import fs from 'fs';
import axios from 'axios';
import cheerio from 'cheerio';
import chalk from 'chalk';
import levenshtein from 'js-levenshtein';
let dataObject = {
    meta: {
        contentUUID: '',
        rawTitle: '', /// the raw title from the jsonl file
        cleanTitle: '', // the title without the // and any text after it and without the year
        coreTitle: '', // the title without : season 1 or : season 2 etc or Part 1 or Part 2, however if it has a : but not season or part after then we should keeop it in
    },
    data: {
        totals: {
            seasonsInNetflixData: 0,
            totalHoursWatched: 0,
           },
           relationships: [{
            itemUUID: '',
            title: '',
            availableGlobally: false, 
            hoursWatched: 0,
            releaseDate: '',
           }],
           sources: {
            imdb: {
                matchedTitle: '',
                matchConfidence: 0,
                details: {
                    title: '',
                    description: '',
                    genres: [],
                    length: '',
                    dates: {
                        singleDate: false,
                        seriesHasEnded: false,
                        singleReleaseDate: '',
                        startDate: '',
                        endDate: ''
                    },
                    contentRating: {
                        parentGuide: {
                            sexNudity: [],
                            violenceGore: [],
                            profanity: [],
                            alcoholDrugsSmoking: [],
                            frighteningIntenseScenes: []
                        },
                        mpaa: '',
                        tvRating: ''
                    },
                    cover: '',
                    userRating: {
                        rating: '',
                        numRatings: '',
                        outOf: ''
                    },
                },
                castCrew: {
                    producers: [{
                        name: ``,
                        imdbLink: ``,
                        role: ``,
                        numEpisodes: ``,
                        date: {
                            dateStart: ``,
                            dateEnd: ``,
                            numYears: ``,
                        }
                    }],
                    directors: [],
                    writers: [],
                    actors: [],
                    crew: []
                },
                keywords: [],
                seasons: [{
                    seasonNumber: 0,
                    episodes: [{
                        episodeNumber: 0,
                        title: '',
                        description: '',
                        length: '',
                        releaseDate: '',
                    }]
                }]
            },
           }
    },
};

const loadData = (filePath) => {
    try {
        const fileContent = fs.readFileSync(filePath, 'utf-8');
        return fileContent.split('\n').filter(line => line.trim() !== '').map(JSON.parse);
    } catch (error) {
        console.error(chalk.red(`Error loading data from file: ${filePath}`), error);
        return [];
    }
};



const calculateMatchConfidence = (title1, title2) => {
    // Remove any content in brackets and trim both titles
    const cleanTitle1 = title1.replace(/\(.*?\)/g, '').trim();
    const cleanTitle2 = title2.replace(/\(.*?\)/g, '').trim();

    // Calculate Levenshtein distance
    const distance = levenshtein(cleanTitle1.toLowerCase(), cleanTitle2.toLowerCase());
    const maxLength = Math.max(cleanTitle1.length, cleanTitle2.length);

    // Normalize and convert to a percentage
    return ((1 - distance / maxLength) * 100).toFixed(2);
};
const getLastScrapedIndex = () => {
    const scrapedManifestFilePath = './data/manifest/scrapedManifest.jsonl';
    let lastScrapedIndex = -1;

    try {
        if (fs.existsSync(scrapedManifestFilePath)) {
            const manifestData = fs.readFileSync(scrapedManifestFilePath, 'utf-8').trim().split('\n');
            if (manifestData.length > 0) {
                const lastLine = manifestData[manifestData.length - 1];
                const lastItem = JSON.parse(lastLine);
                lastScrapedIndex = lastItem.index;
            }
        }
    } catch (error) {
        console.error(chalk.red(`Error reading the last scraped index from manifest: ${error}`));
    }

    return lastScrapedIndex;
};

// Helper function to parse release date
const parseReleaseDate = (releaseDate) => {
    // Check for Ongoing Series (e.g., "2017–")
    if (/^\d{4}[-–—]$/.test(releaseDate)) {
        return {
            singleDate: false,
            seriesOngoing: true,
            seriesHasEnded: false,
            singleReleaseDate: null,
            startDate: releaseDate.slice(0, 4),  // Extract the year
            endDate: null
        };
    }
    // Check for Ended Series (e.g., "2017-2018")
    else if (/^\d{4}[-–—]\d{4}$/.test(releaseDate)) {
        const [startDate, endDate] = releaseDate.split(/[-–—]/).map(s => s.trim());
        return {
            singleDate: false,
            seriesOngoing: false,
            seriesHasEnded: true,
            singleReleaseDate: null,
            startDate: startDate,
            endDate: endDate
        };
    }
    // Check for Single Release Date (e.g., "2017")
    else if (/^\d{4}$/.test(releaseDate)) {
        return {
            singleDate: true,
            seriesOngoing: false,
            seriesHasEnded: false,
            singleReleaseDate: releaseDate,
            startDate: null,
            endDate: null
        };
    }
    // If format does not match any expected pattern
    else {
        return {
            singleDate: null,
            seriesHasEnded: null,
            singleReleaseDate: null,
            startDate: null,
            endDate: null
        };
    }
};

const getImdbInfoFirstPass = async (query, index) => {
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    };

    const timeout = 8000; // 8 seconds timeout

    try {
        const response = await axios.get(`https://www.imdb.com/find?q=${encodeURIComponent(query)}`, { headers, timeout });
        const $ = cheerio.load(response.data);

        const firstResult = $('[data-testid="find-results-section-title"] div.sc-17bafbdb-2 li.ipc-metadata-list-summary-item').first();
        if (firstResult.length === 0) {
            console.error(chalk.yellow(`No valid IMDb result found for query: ${query}`));
            console.log(chalk.dim(`----- Index: ${index} -----`)); // Log the index at the end of the block
            return { status: 'no match found', index, query };
        }

        const matchedTitle = firstResult.find('a').text();
        const imdbLink = `https://www.imdb.com${firstResult.find('a').attr('href').split('?')[0]}`;
        const releaseDate = firstResult.find('.ipc-metadata-list-summary-item__tl li:nth-of-type(1) span').text().trim();
        let type = firstResult.find('li:nth-of-type(2) span').text().trim();
        const dates = parseReleaseDate(releaseDate);

        if (!type && dates.singleDate) {
            type = 'Film';
        }

        const matchConfidence = calculateMatchConfidence(query, matchedTitle);
        let confidenceColor = '';

        if (matchConfidence < 50) {
            confidenceColor = chalk.red;
        } else if (matchConfidence >= 50 && matchConfidence < 90) {
            confidenceColor = chalk.yellow;
        } else {
            confidenceColor = chalk.green;
        }

        console.log(chalk.dim(`\n-----`));
        console.log(chalk.magenta(`Query: ${query}`));
        console.log(chalk.green(`Matched Title: ${matchedTitle}`));
        console.log(chalk.green(`Release Dates: ${releaseDate}`));
        console.log(chalk.green(`Type: ${type}`));
        console.log(confidenceColor(`Match Confidence: ${matchConfidence}%`)); // Apply color based on confidence
        console.log(chalk.dim(`----- Index: ${index} -----`)); // Log the index at the end of the block

        return {
            status: 'success',
            data: {
                matchedTitle: matchedTitle,
                matchConfidence: parseFloat(matchConfidence),
                imdbLink: imdbLink,
                details: {
                    dates: dates,
                    type: type
                }
            },
            index,
            query
        };
    } catch (error) {
        console.error(chalk.red(`Error fetching IMDb info for query: ${query}`), error.message);
        console.log(chalk.dim(`----- Index: ${index} -----`)); // Log the index at the end of the block
        return { status: 'error', message: error.message, index, query, retryable: (error.response && error.response.status === 403) || error.code === 'ECONNABORTED' };
    }
};





const handleImdbResult = (item, imdbResult, currentItemIndex, outputStream, lowConfidenceOutputStream, errorOutputStream, scrapedManifestFilePath) => {
    const manifestEntry = {
        index: currentItemIndex,
        coreTitle: item.meta.coreTitle,
        status: imdbResult.status
    };
    fs.appendFileSync(scrapedManifestFilePath, JSON.stringify(manifestEntry) + '\n');

    if (imdbResult.status === 'success') {
        const updatedItem = { ...item, data: { ...item.data, sources: { ...item.data.sources, imdb: imdbResult.data } }, index: currentItemIndex };

        if (imdbResult.data.matchConfidence >= 70) {
            // Write to scrapedData.jsonl if confidence is 0.7 or higher
            outputStream.write(JSON.stringify(updatedItem) + '\n');
        } else {
            // Write to lowConfidenceData.jsonl if confidence is lower than 0.7
            lowConfidenceOutputStream.write(JSON.stringify(updatedItem) + '\n');
        }
    } else if (imdbResult.status === 'no match found' || imdbResult.status === 'error') {
        errorOutputStream.write(JSON.stringify({ index: currentItemIndex, query: item.meta.coreTitle, status: imdbResult.status, message: imdbResult.message }) + '\n');
    }
};


const processBatch = async (batch, index, outputStream, lowConfidenceOutputStream, errorOutputStream, scrapedManifestFilePath) => {
    const retryDelay = 5000; // Delay for retry
    const maxRetries = 3; // Maximum number of retries

    // Initial processing of all items in the batch in parallel
    const results = await Promise.all(batch.map(async (item, batchIndex) => {
        const currentItemIndex = index + batchIndex;
        console.log(chalk.dim(`Processing item ${currentItemIndex}`));

        if (!item.meta || !item.meta.coreTitle) {
            console.error(`Missing coreTitle for item at index ${currentItemIndex}`);
            return { status: 'error', message: 'Missing coreTitle', index: currentItemIndex, retryable: false };
        }

        return getImdbInfoFirstPass(item.meta.coreTitle, currentItemIndex);
    }));

    // Process items that need retrying
    let retryItems = [];
    for (const result of results) {
        if (result.retryable && result.retryCount < maxRetries) {
            retryItems.push(batch[result.index - index]);
        } else {
            handleImdbResult(batch[result.index - index], result, result.index, outputStream, lowConfidenceOutputStream, errorOutputStream, scrapedManifestFilePath);
        }
    }

    // Retry failed items
    for (let retryCount = 0; retryCount < maxRetries; retryCount++) {
        if (retryItems.length === 0) {
            break; // No more items to retry
        }

        console.log(chalk.yellow(`Retrying ${retryItems.length} items. Retry attempt ${retryCount + 1}`));
        await sleep(retryDelay); // Delay before retrying

        const retryResults = await Promise.all(retryItems.map(async (item) => {
            const currentItemIndex = item.index;
            return getImdbInfoFirstPass(item.meta.coreTitle, currentItemIndex, retryCount + 1);
        }));

        retryItems = []; // Reset for next round of retries
        for (const result of retryResults) {
            if (result.retryable && result.retryCount < maxRetries) {
                retryItems.push(batch[result.index - index]);
            } else {
                handleImdbResult(batch[result.index - index], result, result.index, outputStream, lowConfidenceOutputStream, errorOutputStream, scrapedManifestFilePath);
            }
        }
    }

    // Handle items that exceeded retry attempts
    for (const item of retryItems) {
        errorOutputStream.write(JSON.stringify({ index: item.index, query: item.meta.coreTitle, status: 'error', message: 'Exceeded retry attempts' }) + '\n');
    }
};


const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));


const processFirstPass = async (devMode = false) => {
    const startIndex = getLastScrapedIndex() + 1;
    const batchSize = 10;
    let dataObjects = loadData('./data/processed/sortedData.jsonl');

    if (devMode) {
        console.log(chalk.yellow('*** DEV MODE ON. CHOOSING 10 RANDOM OBJECTS ***'));
        dataObjects = dataObjects.sort(() => 0.5 - Math.random()).slice(0, 10);
    }

    const outputFilePath = './data/processed/scrapedData.jsonl';
    const lowConfidenceFilePath = './data/processed/lowConfidenceData.jsonl';
    const errorFilePath = './data/errors/errorData.jsonl';
    const scrapedManifestFilePath = './data/manifest/scrapedManifest.jsonl';

    const outputStream = fs.createWriteStream(outputFilePath, { flags: 'a' });
    const lowConfidenceOutputStream = fs.createWriteStream(lowConfidenceFilePath, { flags: 'a' });
    const errorOutputStream = fs.createWriteStream(errorFilePath, { flags: 'a' });

    let shouldDelay = false;

    for (let index = startIndex; index < dataObjects.length; index += batchSize) {
        if (shouldDelay) {
            console.log(chalk.red('Received 403 response. Waiting for 30 seconds before the next batch.'));
            await sleep(20000);
            shouldDelay = false;
        }

        const batch = dataObjects.slice(index, index + batchSize);
        shouldDelay = await processBatch(batch, index, outputStream, lowConfidenceOutputStream, errorOutputStream, scrapedManifestFilePath);

        // Wait for 1 to 3 seconds randomly after each batch
        const waitTime = Math.random() * (1000 - 3000) + 3000;
        console.log(chalk.blue(`Waiting for ${waitTime / 1000} seconds before next batch...`));
        await sleep(waitTime);
    }

    outputStream.end();
    lowConfidenceOutputStream.end();
    errorOutputStream.end();
    console.log(chalk.green('Processing complete.'));
};

export const main = async () => {
    await processFirstPass();
};

main();
