import fs from 'fs';
import axios from 'axios';
import cheerio from 'cheerio';
import chalk from 'chalk';
import levenshtein from 'js-levenshtein';
import readline from 'readline';
import { v4 as uuidv4 } from 'uuid';
import path from 'path';


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




// Helper Processing Functions 
const initFilePaths = () => {
    const paths = {
        outputFilePath: './data/processed/scrapedData.jsonl',
        lowConfidenceFilePath: './data/processed/lowConfidenceData.jsonl',
        errorFilePath: './data/errors/errorData.jsonl',
        minOutputFilePath: './data/processed/scrapedDataMin.jsonl',
        scrapedManifestFilePath: './data/manifest/scrapedManifest.jsonl'
    };

    for (let filePath of Object.values(paths)) {
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
    }

    return paths;
};
const manifestWrite = async (data) => {
    const filePath = './data/manifest/scrapedManifest.jsonl';
    const minimalData = {
        status: data.status,
        indexNum: data.indexNum,
        coreTitle: data.meta.coreTitle,
        imdbLink: data.imdbLink // Assuming this is provided in the data passed to manifestWrite
    };
    const stream = fs.createWriteStream(filePath, {flags: 'a'});
    stream.write(`${JSON.stringify(minimalData)}\n`);
    stream.end();
};
const getLastScrapedItem = async () => {
    const filePath = './data/manifest/scrapedManifest.jsonl';
    if (!fs.existsSync(filePath) || fs.statSync(filePath).size === 0) {
        return null;
    }

    const data = fs.readFileSync(filePath, 'utf8').trim().split('\n').map(line => {
        const entry = JSON.parse(line);
        // Ensure indexNum is treated as an integer for accurate comparisons
        entry.indexNum = parseInt(entry.indexNum, 10);
        return entry;
    });

    if (data.length === 0) {
        return null;
    }

    // Use reduce to find the entry with the highest indexNum
    const highestIndexEntry = data.reduce((acc, curr) => (acc.indexNum > curr.indexNum ? acc : curr), data[0]);

    // No need to check for meta object existence as we're directly accessing indexNum and coreTitle
    return highestIndexEntry ? {
        coreTitle: highestIndexEntry.coreTitle, // Assuming direct access, adjust if nested
        indexNum: highestIndexEntry.indexNum
    } : null;
};




const loadInitData = async () => {
    const sortedDataFilePath = './data/processed/sortedData.jsonl';
    // Get the last scraped item index number
    const lastScrapedItem = await getLastScrapedItem();
    const startIndex = lastScrapedItem ? lastScrapedItem.indexNum + 1 : 0; // Start from the beginning if no last item
    
    console.log(`Starting from index number: ${startIndex}`);
    return startIndex; // This value will be used to start processing in processAllBatches
};
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));


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
const getImdbGeneralInfo = async (imdbLink) => {
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    };

    const attemptRequest = async () => {
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
            if (error.code === 'ECONNRESET') {
                console.log('Socket hang up - trying again');
                await new Promise(resolve => setTimeout(resolve, 1000)); // Wait for 1 second
                return attemptRequest(); // Try again
            } else {
                console.error('Error scraping IMDb data:', error);
                return null;
            }
        }
    };

    return attemptRequest();
};
const getCastCrew = async (imdbLink) => {
    const fullCreditsUrl = imdbLink + 'fullcredits';
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    };

    const attemptRequest = async () => {
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
            if (error.code === 'ECONNRESET') {
                console.log('Socket hang up - trying again');
                await new Promise(resolve => setTimeout(resolve, 1000)); // Wait for 1 second
                return attemptRequest(); // Try again
            } else {
                console.error('Error scraping IMDb cast and crew data:', error);
                return null;
            }
        }
    };

    return attemptRequest();
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
        


        console.log(chalk.dim(`\n-----`));
        console.log(chalk.magenta(`Query: ${query}`));
        console.log(chalk.green(`Matched Title: ${matchedTitle}`));
        console.log(chalk.green(`Release Dates: ${releaseDate}`));
        console.log(chalk.green(`Type: ${type}`));
        console.log(confidenceColor(`Match Confidence: ${matchConfidence}%`));

        // Only proceed with fetching general info and cast & crew if match confidence is over 70%
        if (matchConfidence > 70) {
            console.log(chalk.dim('Fetching Additional General Data for Title:'), chalk.green(matchedTitle), chalk.yellow(imdbLink));
            generalInfoData = await getImdbGeneralInfo(imdbLink);
            castCrewData = await getCastCrew(imdbLink);
        } else {
            console.log(chalk.red('Match confidence Low. Skipping additional scraping.'));
        }

        return {
            status: 'success',
            data: {
                matchedTitle: matchedTitle,
                matchConfidence: parseFloat(matchConfidence),
                imdbLink: imdbLink,
                details: {
                    title: generalInfoData.title || matchedTitle, // Fallback to matchedTitle if not available
                    description: generalInfoData.description || '', // Assuming description is part of the generalInfoData
                    genres: generalInfoData.genres || [],
                    length: generalInfoData.length || '',
                    dates: {
                        singleDate: dates.singleDate || false,
                        seriesHasEnded: dates.seriesHasEnded || false, // Assuming you can determine this from the fetched data
                        singleReleaseDate: dates.singleReleaseDate || releaseDate, // Fallback to releaseDate if not available
                        startDate: dates.startDate || '', // Assuming startDate is part of dates
                        endDate: dates.endDate || '' // Assuming endDate is part of dates
                    },
                    contentRating: {
                        // Assuming these ratings are part of the generalInfoData or another source
                        parentGuide: {
                            sexNudity: generalInfoData.sexNudity || [],
                            violenceGore: generalInfoData.violenceGore || [],
                            profanity: generalInfoData.profanity || [],
                            alcoholDrugsSmoking: generalInfoData.alcoholDrugsSmoking || [],
                            frighteningIntenseScenes: generalInfoData.frighteningIntenseScenes || []
                        },
                        mpaa: generalInfoData.mpaa || '',
                        tvRating: generalInfoData.tvRating || ''
                    },
                    cover: generalInfoData.cover || '',
                    userRating: {
                        rating: generalInfoData.imdbRating || '',
                        numRatings: generalInfoData.numRatings || '',
                        outOf: generalInfoData.outOf || '' // Assuming outOf is a part of the generalInfoData
                    },
                },
                castCrew: castCrewData // Assuming castCrewData is structured as required
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





// Main Processing Functions
const processItem = async (item) => {
    if (!item.meta || !item.meta.coreTitle) {
        throw new Error(`Missing coreTitle for item at index ${item.indexNum}`);
    }

    const result = await fetchInfoIMDB(item.meta.coreTitle, item.indexNum);

    const minimalData = {
        status: result.status,
        indexNum: item.indexNum,
        coreTitle: item.meta.coreTitle,
        imdbLink: result.data ? result.data.imdbLink : null,
        matchedTitle: result.data ? result.data.matchedTitle : '',
        matchConfidence: result.data ? result.data.matchConfidence : 0,
        releaseDate: result.data ? result.data.details.dates : {},
        type: result.data ? result.data.type : '',
    };

    const minOutputFilePath = './data/processed/scrapedDataMin.jsonl';
    fs.appendFileSync(minOutputFilePath, `${JSON.stringify(minimalData)}\n`);

    // Append full result to the processed data file
    const outputFilePath = './data/processed/scrapedData.jsonl';
    fs.appendFileSync(outputFilePath, `${JSON.stringify(result)}\n`);

    // Prepare data for the manifest, including only required properties
    await manifestWrite({
        status: 'Success',
        indexNum: item.indexNum,
        meta: { coreTitle: item.meta.coreTitle },
        imdbLink: result.data ? result.data.imdbLink : null,
        matchConfidence: result.data ? result.data.matchConfidence : null
    });
};
const processBatch = async (batch) => {
    const errorFilePath = './data/errors/errorData.jsonl'; // Assuming this is defined in your paths
    const maxRetries = 3;
    const retryDelay = 5000; // Delay in milliseconds
    const concurrentRequests = 5;

    // Function to process a single item with retries
    const processWithRetries = async (item) => {
        let retryCount = 0;
        while (retryCount <= maxRetries) {
            try {
                await processItem(item);
                return { success: true }; // Exit the loop on success
            } catch (error) {
                if (retryCount < maxRetries) {
                    console.error(chalk.yellow(`Error processing item at index ${item.indexNum}. Retry attempt ${retryCount + 1}`), error);
                    await sleep(retryDelay);
                    retryCount++;
                } else {
                    console.error(chalk.red(`Failed after ${maxRetries} retries for item at index ${item.indexNum}:`), error);
                    fs.appendFileSync(errorFilePath, `${JSON.stringify({ ...item, error: error.message, retryCount })}\n`);
                    return { success: false, error: `Failed after ${maxRetries} retries for item at index ${item.indexNum}: ${error.message}` };
                }
            }
        }
    };

    // Split the batch into chunks of concurrentRequests
    for (let i = 0; i < batch.length; i += concurrentRequests) {
        const chunk = batch.slice(i, i + concurrentRequests);
        // Process each chunk concurrently
        await Promise.all(chunk.map(item => processWithRetries(item)));
    }
};
const countTotalItems = async (filePath) => {
    return new Promise((resolve, reject) => {
      let count = 0;
      fs.createReadStream(filePath)
        .on('data', (chunk) => {
          count += chunk.toString().split('\n').filter(Boolean).length; // Count non-empty lines
        })
        .on('end', () => resolve(count))
        .on('error', reject);
    });
}; 
const processAllBatches = async () => {
    const batchSize = 10;
    
    const sortedDataFilePath = './data/processed/sortedData.jsonl';

    // Function to generate a random delay between 1 and 4 seconds
    const randomDelay = () => Math.floor(Math.random() * (4000 - 1000 + 1)) + 1000;

    const totalItems = await countTotalItems(sortedDataFilePath);
    console.log(`Total items in file: ${totalItems}`);

    const startIndex = await loadInitData(); // Ensures starting from the correct index
    console.log(`Starting from index number: ${startIndex}`);

    const stream = fs.createReadStream(sortedDataFilePath);
    const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
    });

    let currentBatch = [];
    let firstIndexInBatch = startIndex;
    let itemsProcessed = 0; // Keep track of items processed to adjust first index in each new batch

    for await (const line of rl) {
        const item = JSON.parse(line);
        const currentIndex = item.indexNum;

        if (currentIndex >= startIndex) {
            currentBatch.push(item);
            if (currentBatch.length === 1) {
                firstIndexInBatch = currentIndex; // Update firstIndexInBatch at the start of a new batch
            }

            if (currentBatch.length === batchSize || rl.readableEnded) {
                const batchEndIndex = currentBatch.length < batchSize ? firstIndexInBatch : firstIndexInBatch + currentBatch.length - 1;
                console.log(chalk.magenta(`Processing Items (${firstIndexInBatch} - ${batchEndIndex}) of total ${totalItems}.`));

                await processBatch(currentBatch);
                itemsProcessed += currentBatch.length;
                firstIndexInBatch += currentBatch.length; // Prepare firstIndexInBatch for the next batch
                currentBatch = []; // Reset the batch

                const delay = randomDelay();
                console.log(`Waiting for ${delay / 1000} seconds before processing the next batch...`);
                await sleep(delay);
            }
        }
    }

    console.log(chalk.green('All batches processed.'));
};





const main = async () => {
    console.log('Initializing file paths...');
    const paths = initFilePaths(); // This ensures all directories and files are ready before processing.
    console.log(paths); // Optionally log the paths for verification.

    console.log('Starting data processing...');
    await processAllBatches();
    console.log('Data processing completed.');
};

main();
