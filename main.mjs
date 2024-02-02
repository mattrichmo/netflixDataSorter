import fs from 'fs';
import axios from 'axios';
import cheerio from 'cheerio';
import chalk from 'chalk';
import levenshtein from 'js-levenshtein';
import readline from 'readline';
import { v4 as uuidv4 } from 'uuid';
import path from 'path';
import { fileURLToPath } from 'url';


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
    return new Promise((resolve, reject) => {
        const dataObjects = [];
        const readStream = fs.createReadStream(filePath, 'utf-8');
        const lineReader = readline.createInterface({
            input: readStream,
            crlfDelay: Infinity
        });

        lineReader.on('line', (line) => {
            if (line.trim() !== '') {
                try {
                    dataObjects.push(JSON.parse(line));
                } catch (error) {
                    console.error(`Error parsing line: ${error}`);
                }
            }
        });

        lineReader.on('close', () => {
            resolve(dataObjects);
        });

        lineReader.on('error', (error) => {
            reject(error);
        });
    });
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
//helper function to calculate match confidence using levenshtein distance
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


//Imdb Scraping Functions

function parseCreditInfo(creditString, defaultRole = '') {
    const regex = /^(.*?)(?:\s*\((\d+)\s*episodes\))?(?:\s*(\d{4})-(\d{4}|\s*))?$/;
    const match = regex.exec(creditString);
    const role = match[1] || defaultRole;
    const numEpisodes = match[2] || '';
    const dateStart = match[3] || '';
    const dateEnd = match[4] || '';
    const numYears = dateEnd ? (parseInt(dateEnd) - parseInt(dateStart)).toString() : '';

    return {
        role,
        numEpisodes,
        date: {
            dateStart,
            dateEnd,
            numYears
        }
    };
};
const getImdbGeneralInfo = async (matchedTitle, imdbLink) => {

    console.log(chalk.dim('Fetching IMDb General Data for Title:'), chalk.green(matchedTitle), chalk.yellow(imdbLink));

    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    };

    try {
        const response = await axios.get(imdbLink, { headers });
        const html = response.data;
        const $ = cheerio.load(html);

        const title = $('span.hero__primary-text').text().trim();
        const imdbRating = $('.sc-69e49b85-1 div.sc-bde20123-0 span.sc-bde20123-1').first().text().trim();
        const numRatings = $('.sc-69e49b85-1 div.sc-bde20123-3').first().text().trim();
        const outOf = $('.sc-69e49b85-1 div.sc-bde20123-0 span:nth-of-type(2)').first().text().trim();
        const length = $('.sc-d8941411-2 li:nth-of-type(4)').text().trim();
        const dates = $('.sc-d8941411-2 li:nth-of-type(2) a').text().trim();
        const showRating = $('.sc-d8941411-2 li:nth-of-type(3) a').text().trim();
        const cover = $('.ipc-media--poster-l img').attr('src').trim();

        let genres = [];
        $('.ipc-chip-list--baseAlt div.ipc-chip-list__scroller span').each((i, element) => {
            genres.push($(element).text().trim());
        });


        const imdbObject = {
            title,
            imdbRating,
            numRatings,
            outOf,
            length,
            dates,
            showRating,
            cover,
            genres,
        };

        return imdbObject;
    } catch (error) {
        console.error('Error scraping IMDb data:', error);
        return null;
    }
};
const getCastCrew = async (imdbLink, matchedTitle) => {
    const fullCreditsUrl = imdbLink + 'fullcredits';
    console.log(chalk.dim('FetchingCast & Crew Data for Title:'), chalk.green(matchedTitle), chalk.yellow(imdbLink));

    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    };

    try {
        const creditsResponse = await axios.get(fullCreditsUrl, { headers });
        const creditsHtml = creditsResponse.data;
        const $ = cheerio.load(creditsHtml);

        let castCrew = {
            producers: [],
            directors: [],
            writers: [],
            actors: [],
            crew: []
        };

        const categories = ['directors', 'writers', 'actors', 'producers', 'crew'];
        const roles = ['director', 'writer', 'actor', 'producer', 'crew member'];
        const selectors = ['table:nth-of-type(1) tr', 'table:nth-of-type(2) tr', '.cast_list tr', 'table:nth-of-type(4) tr', 'table:nth-of-type(n+5) tr'];

        categories.forEach((category, index) => {
            $(selectors[index]).each((i, elem) => {
                const name = $(elem).find('td.name a').text().trim();
                const link = `https://www.imdb.com${$(elem).find('td.name a').attr('href')}`;
                const creditDetails = $(elem).find('td.credit').text().trim();
                if (name) {
                    const parsedData = parseCreditInfo(creditDetails, roles[index]);
                    castCrew[category].push({ uuid: uuidv4(), name, imdbLink: link, ...parsedData });
                }
            });
        });

        return castCrew;
    } catch (error) {
        console.error('Error scraping IMDb cast and crew data:', error);
        return null;
    }
};
const searchImdb = async (query) => {
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    };

    const timeout = 15000; // 15 seconds timeout

    try {
        const response = await axios.get(`https://www.imdb.com/find?q=${encodeURIComponent(query)}`, { headers, timeout });
        const $ = cheerio.load(response.data);

        const firstResult = $('[data-testid="find-results-section-title"] div.sc-17bafbdb-2 li.ipc-metadata-list-summary-item').first();
        if (firstResult.length === 0) {
            console.error(chalk.yellow(`No valid IMDb result found for query: ${query}`));
            return null;
        }

        const matchedTitle = firstResult.find('a').text();
        const imdbId = firstResult.find('a').attr('href').split('/')[2];
        const imdbLink = `https://www.imdb.com/title/${imdbId}/`;
        const releaseDate = firstResult.find('.ipc-metadata-list-summary-item__tl li:nth-of-type(1) span').text().trim();
        let type = firstResult.find('li:nth-of-type(2) span').text().trim();
        const dates = parseReleaseDate(releaseDate);

        if (!type && dates.singleDate) {
            type = 'Film';
        }

        return {
            matchedTitle,
            imdbId,
            imdbLink,
            releaseDate,
            type,
            dates
        };
    } catch (error) {
        console.error(chalk.red(`Error searching IMDb for query: ${query}`), error.message);
        return null; // or throw the error, depending on how you want to handle errors
    }
};
// Main function to fetch IMDb info Logic
const fetchInfoIMDB = async (query, index) => {
    try {
        const searchResult = await searchImdb(query);
        if (!searchResult) {
            console.log(chalk.dim(`----- Index: ${index} -----`));
            return { status: 'no match found', index, query };
        }

        const { matchedTitle, imdbLink, releaseDate, type, dates } = searchResult;
        const matchConfidence = calculateMatchConfidence(query, matchedTitle);
        let confidenceColor = matchConfidence < 50 ? chalk.red : matchConfidence < 90 ? chalk.yellow : chalk.green;

        let generalInfoData = {};
        let castCrewData = {};
        
        // Only proceed with fetching general info and cast & crew if match confidence is over 70%
        if (matchConfidence > 70) {
            generalInfoData = await getImdbGeneralInfo(matchedTitle, imdbLink);
            castCrewData = await getCastCrew(imdbLink, matchedTitle);
        }

        console.log(chalk.dim(`\n-----`));
        console.log(chalk.magenta(`Query: ${query}`));
        console.log(chalk.green(`Matched Title: ${matchedTitle}`));
        console.log(chalk.green(`Release Dates: ${releaseDate}`));
        console.log(chalk.green(`Type: ${type}`));
        console.log(confidenceColor(`Match Confidence: ${matchConfidence}%`));

        return {
            status: 'success',
            data: {
                matchedTitle: matchedTitle,
                matchConfidence: parseFloat(matchConfidence),
                imdbLink: imdbLink,
                details: {
                    title: generalInfoData.title || '',
                    description: '', // Populate as necessary
                    genres: generalInfoData.genres || [],
                    length: generalInfoData.length || '',
                    dates: dates,
                    contentRating: {
                    },
                    cover: generalInfoData.cover || '',
                    userRating: {
                        rating: generalInfoData.imdbRating || '',
                        numRatings: generalInfoData.numRatings || 0,
                        outOf: generalInfoData.outOf || 0
                    },
                },
                castCrew: castCrewData
            },
            index,
            query
        };
    } catch (error) {
        console.error(chalk.red(`Error in fetchInfoIMDB for query: ${query}`), error.message);
        console.log(chalk.dim(`----- Index: ${index} -----`));
        return { status: 'error', message: error.message, index, query, retryable: (error.response && error.response.status === 403) || error.code === 'ECONNABORTED' };
    }
};



//Helper Processing Functions
const getLastScrapedIndex = async () => {
    const scrapedManifestFilePath = './data/manifest/scrapedManifest.jsonl';
    let lastScrapedIndex = -1;

    try {
        // Create the directory path if it doesn't exist
        const dirPath = path.dirname(scrapedManifestFilePath);
        if (!fs.existsSync(dirPath)) {
            fs.mkdirSync(dirPath, { recursive: true });
        }

        if (fs.existsSync(scrapedManifestFilePath)) {
            const manifestData = fs.readFileSync(scrapedManifestFilePath, 'utf-8').trim().split('\n');
            lastScrapedIndex = manifestData.length - 1; // Use the total number of items as the last index
        }
    } catch (error) {
        console.error(chalk.red(`Error reading the last scraped index from manifest: ${error}`));
    }

    return lastScrapedIndex;
};

const handleImdbResult = async (item, imdbResult, currentItemIndex, outputStream, lowConfidenceOutputStream, errorOutputStream, minDataStream, scrapedManifestFilePath) => {
    const manifestEntry = {
        index: currentItemIndex,
        coreTitle: item.meta.coreTitle,
        status: imdbResult.status
    };
    
    // Create a writable stream for the scraped manifest file
    const manifestStream = fs.createWriteStream(scrapedManifestFilePath, {flags: 'a'});

    // Write the manifest entry to the stream
    manifestStream.write(JSON.stringify(manifestEntry) + '\n');

    if (imdbResult.status === 'success') {
        // Clone the item and remove cast and crew from the data
        const minimizedItem = JSON.parse(JSON.stringify(item));
        if (minimizedItem.data && minimizedItem.data.sources && minimizedItem.data.sources.imdb) {
            delete minimizedItem.data.sources.imdb.cast;
            delete minimizedItem.data.sources.imdb.crew;
        }

        const updatedItem = { ...item, data: { ...item.data, sources: { ...item.data.sources, imdb: imdbResult.data } }, index: currentItemIndex };

        if (imdbResult.data.matchConfidence >= 70) {
            outputStream.write(JSON.stringify(updatedItem) + '\n');
            // Also write the minimized item to the new file
            minDataStream.write(JSON.stringify(minimizedItem) + '\n');
        } else {
            lowConfidenceOutputStream.write(JSON.stringify(updatedItem) + '\n');
            // Also write the minimized item to the low confidence file, if needed
        }
    } else if (imdbResult.status === 'no match found' || imdbResult.status === 'error') {
        errorOutputStream.write(JSON.stringify({ index: currentItemIndex, query: item.meta.coreTitle, status: imdbResult.status, message: imdbResult.message }) + '\n');
    }
};
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const initializeFilePathsAndPrepareFileSystem = async () => {
    // Define all the necessary file paths
    const paths = {
        outputFilePath: './data/processed/scrapedData.jsonl',
        lowConfidenceFilePath: './data/processed/lowConfidenceData.jsonl',
        errorFilePath: './data/errors/errorData.jsonl',
        minOutputFilePath: './data/processed/scrapedDataMin.jsonl',
        scrapedManifestFilePath: './data/manifest/scrapedManifest.jsonl'
    };

    // Ensure all required directories and files are prepared
    await ensureFileSystemPrepared(Object.values(paths));

    return paths;
};
const ensureFileSystemPrepared = async (filePaths) => {
    filePaths.forEach(filePath => {
        const dirPath = path.dirname(filePath);
        if (!fs.existsSync(dirPath)) {
            fs.mkdirSync(dirPath, { recursive: true });
            console.log(chalk.green(`Directory created: ${dirPath}`));
        }

        // Ensure the file exists to prevent errors in file operations
        if (!fs.existsSync(filePath)) {
            fs.writeFileSync(filePath, '');
            console.log(chalk.green(`File created: ${filePath}`));
        }
    });
};

//Main Processing Functions
const processBatch = async (batch, index, outputStream, lowConfidenceOutputStream, errorOutputStream, minOutputStream, scrapedManifestFilePath) => {
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

        return fetchInfoIMDB(item.meta.coreTitle, currentItemIndex);
    }));

    // Process items that need retrying
    let retryItems = [];
    for (const result of results) {
        if (result.retryable && result.retryCount < maxRetries) {
            retryItems.push(batch[result.index - index]);
        } else {
            handleImdbResult(batch[result.index - index], result, result.index, outputStream, lowConfidenceOutputStream, errorOutputStream, minOutputStream, scrapedManifestFilePath);
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
            return fetchInfoIMDB(item.meta.coreTitle, currentItemIndex, retryCount + 1);
        }));

        retryItems = []; // Reset for next round of retries
        for (const result of retryResults) {
            if (result.retryable && result.retryCount < maxRetries) {
                retryItems.push(batch[result.index - index]);
            } else {
                handleImdbResult(batch[result.index - index], result, result.index, outputStream, lowConfidenceOutputStream, errorOutputStream, minOutputStream, scrapedManifestFilePath);
                        }
        }
    }

    // Handle items that exceeded retry attempts
    for (const item of retryItems) {
        errorOutputStream.write(JSON.stringify({ index: item.index, query: item.meta.coreTitle, status: 'error', message: 'Exceeded retry attempts' }) + '\n');
    }
};

const processFirstPass = async (devMode = false) => {
    // Initialize file paths and ensure filesystem is prepared
    const {
        outputFilePath,
        lowConfidenceFilePath,
        errorFilePath,
        minOutputFilePath,
        scrapedManifestFilePath
    } = await initializeFilePathsAndPrepareFileSystem();

    const startIndex = await getLastScrapedIndex() + 1;
    console.log(chalk.blue(`Starting processing from index: ${startIndex}`));

    const batchSize = 5;

    const outputStream = fs.createWriteStream(outputFilePath, { flags: 'a' });
    const lowConfidenceOutputStream = fs.createWriteStream(lowConfidenceFilePath, { flags: 'a' });
    const errorOutputStream = fs.createWriteStream(errorFilePath, { flags: 'a' });
    const minOutputStream = fs.createWriteStream(minOutputFilePath, { flags: 'a' });

    let shouldDelay = false;
    let currentIndex = startIndex;
    let batch = [];

    const readStream = fs.createReadStream('./data/processed/sortedData.jsonl', 'utf-8');
    const lineReader = readline.createInterface({
        input: readStream,
        crlfDelay: Infinity
    });

    for await (const line of lineReader) {
        if (currentIndex >= startIndex) {
            if (line.trim() !== '') {
                try {
                    const dataObject = JSON.parse(line);
                    batch.push(dataObject);

                    if (batch.length === batchSize || devMode) {
                        if (shouldDelay) {
                            console.log(chalk.red('Received 403 response. Waiting for 30 seconds before the next batch.'));
                            await sleep(20000);
                            shouldDelay = false;
                        }

                        // Process the current batch
                        await processBatch(batch, currentIndex, outputStream, lowConfidenceOutputStream, errorOutputStream, minOutputStream, scrapedManifestFilePath);

                        // Wait for 1 to 3 seconds randomly after each batch
                        const waitTime = Math.random() * (2000 - 1000) + 1000;
                        console.log(chalk.blue(`Waiting ${waitTime / 1000} seconds before running next batch ->`));
                        await sleep(waitTime);

                        currentIndex += batch.length; // Update currentIndex after processing each batch
                        batch = []; // Clear the batch for the next set of items
                    }
                } catch (error) {
                    console.error(chalk.red(`Error parsing line: ${error}`));
                }
            }
        } else {
            currentIndex++; // Increment currentIndex if not processing yet
        }

        if (devMode && currentIndex - startIndex >= 10) {
            break; // In dev mode, process only 10 items
        }
    }

    // Process any remaining items in the last batch
    if (batch.length > 0) {
        await processBatch(batch, currentIndex, outputStream, lowConfidenceOutputStream, errorOutputStream, minOutputStream, scrapedManifestFilePath);
    }

    outputStream.end();
    lowConfidenceOutputStream.end();
    errorOutputStream.end();
    minOutputStream.end(); // Ensure to close the new stream as well

    console.log(chalk.green('Processing complete.'));
};



export const main = async () => {
    await processFirstPass();
};

main();
