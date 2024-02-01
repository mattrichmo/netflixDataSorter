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
// Function to read the last scraped index from the manifest
const getLastScrapedIndex = () => {
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
    console.log(chalk.dim(`Fetching IMDb info for query:`), chalk.magenta(`${query}`));

    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    };

    const timeout = 8000; // 8 seconds timeout
    const hardRetryTimeout = 30000; // 10 seconds hard retry timeout for 403 error

    try {
        const response = await axios.get(`https://www.imdb.com/find?q=${encodeURIComponent(query)}`, { headers, timeout });
        const $ = cheerio.load(response.data);

        const firstResult = $('[data-testid="find-results-section-title"] div.sc-17bafbdb-2 li.ipc-metadata-list-summary-item').first();
        if (firstResult.length === 0) {
            console.error(chalk.yellow(`No valid IMDb result found for query: ${query}`));
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
        console.log(chalk.dim(`\n-----`));
        console.log(chalk.magenta(`Query: ${query}`));
        if (matchConfidence >= 90) {
            console.log(chalk.green(`Matched Title: ${matchedTitle}`));
            console.log(chalk.green(`Release Dates: ${releaseDate}`));
            console.log(chalk.green(`Type: ${type}`));
            console.log(chalk.green(`Match Confidence: ${matchConfidence}%`));
        } else if (matchConfidence < 50) {
            console.log(chalk.red(`Matched Title: ${matchedTitle}`));
            console.log(chalk.red(`Release Dates: ${releaseDate}`));
            console.log(chalk.red(`Type: ${type}`));
            console.log(chalk.red(`Match Confidence: ${matchConfidence}%`));
        } else {
            console.log(chalk.yellow(`Matched Title: ${matchedTitle}`));
            console.log(chalk.yellow(`Release Dates: ${releaseDate}`));
            console.log(chalk.yellow(`Type: ${type}`));
            console.log(chalk.yellow(`Match Confidence: ${matchConfidence}%`));
        }
        console.log(chalk.dim(`-----`));

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
        return { status: 'error', message: error.message, index, query };
    }
};

const folderFileInit = () => {

    const inputFilePath = './data/processed/sortedData.jsonl';
    const outputFilePath = './data/processed/scrapedData.jsonl';
    const lowConfidenceFilePath = './data/processed/lowConfidenceData.jsonl';
    const errorFilePath = './data/errors/errorData.jsonl';
    const manifestFilePath = './data/manifest/scrapedManifest.jsonl';

    if (!fs.existsSync(errorDirectory)){
        fs.mkdirSync(errorDirectory, { recursive: true });
    }

    if (!fs.existsSync(manifestDirectory)){
        fs.mkdirSync(manifestDirectory, { recursive: true });
    }

};


const processFirstPass = async (devMode) => {


    const inputFilePath = './data/processed/sortedData.jsonl';
    const outputFilePath = './data/processed/scrapedData.jsonl';
    const lowConfidenceFilePath = './data/processed/lowConfidenceData.jsonl';
    const errorDirectory = './data/errors'; 
    const manifestDirectory = './data/manifest'; 



    const errorFilePath = `${errorDirectory}/errorData.jsonl`; 
    const allLines = fs.readFileSync(inputFilePath, 'utf-8').split('\n').filter(line => line.trim() !== '');
    let dataObjects = allLines.map(line => JSON.parse(line));

    if (devMode) {
        dataObjects = dataObjects.sort(() => 0.5 - Math.random()).slice(0, 10);
        console.log(chalk.yellow('*** DEV MODE ON. CHOOSING 10 RANDOM OBJECTS ***'));
    }

    const lastScrapedIndex = getLastScrapedIndex();
    console.log(chalk.green(`Found items in manifest. Last scraped index: ${lastScrapedIndex}. Starting from the next item.`));

    const scrapedManifestFilePath = `${manifestDirectory}/scrapedManifest.jsonl`; 
    const scrapedIndicesSet = new Set();

    if (fs.existsSync(scrapedManifestFilePath)) {
        const manifestLines = fs.readFileSync(scrapedManifestFilePath, 'utf-8').split('\n').filter(line => line.trim() !== '');
        manifestLines.forEach(line => {
            const { index } = JSON.parse(line);
            scrapedIndicesSet.add(index);
        });
    }

    const outputStream = fs.createWriteStream(outputFilePath, { flags: 'a' });
    const lowConfidenceOutputStream = fs.createWriteStream(lowConfidenceFilePath, { flags: 'a' });
    const errorOutputStream = fs.createWriteStream(errorFilePath, { flags: 'a' });

    let currentIndex = lastScrapedIndex;
    let shouldDelay = false; // Flag to indicate if we need to delay the next batch

    for (let index = 0; index < dataObjects.length; index += 10) {
        if (shouldDelay) {
            console.log(chalk.red('Received 403 response. Waiting for 30 seconds before the next batch.'));
            await new Promise(resolve => setTimeout(resolve, 30000)); // Wait for 30 seconds
            shouldDelay = false; // Reset the flag
        }

        const batch = dataObjects.slice(index, index + 10);

        const imdbPromises = batch.map(async (dataObject, batchIndex) => {
            const currentItemIndex = currentIndex + batchIndex + 1;

            if (devMode || (!scrapedIndicesSet.has(currentItemIndex) && currentItemIndex > lastScrapedIndex)) {
                console.log(chalk.dim(`Processing item ${currentItemIndex} of ${dataObjects.length}`));
                try {
                    const imdbResult = await getIMDBinfo(dataObject.meta.coreTitle, currentItemIndex);

                    // Update manifest and handle results
                    fs.appendFileSync(scrapedManifestFilePath, JSON.stringify({ index: currentItemIndex, status: imdbResult.status }) + '\n');

                    if (imdbResult.status === 'success' && imdbResult.data.matchConfidence >= 50) {
                        dataObject.data.sources.imdb = imdbResult.data;
                        outputStream.write(JSON.stringify({ ...dataObject, index: currentItemIndex }) + '\n');
                    } else if (imdbResult.status === 'no match found') {
                        errorOutputStream.write(JSON.stringify({ index: currentItemIndex, query: dataObject.meta.coreTitle, status: 'no match found' }) + '\n');
                    } else if (imdbResult.status === 'error') {
                        errorOutputStream.write(JSON.stringify({ index: currentItemIndex, query: dataObject.meta.coreTitle, status: 'error', message: imdbResult.message }) + '\n');
                    } else {
                        lowConfidenceOutputStream.write(JSON.stringify({ ...dataObject, index: currentItemIndex }) + '\n');
                    }
                } catch (error) {
                    if (error.response && error.response.status === 403) {
                        shouldDelay = true; // Set the flag to delay next batch
                    }
                    // Handle other errors or log them
                    console.error(`Error processing item ${currentItemIndex}:`, error.message);
                }
                return;
            }
            return null;
        });

        await Promise.all(imdbPromises);
        currentIndex += 10; // Increment currentIndex for the next batch
    }

    outputStream.end();
    lowConfidenceOutputStream.end();
    errorOutputStream.end();
    console.log(chalk.green('IMDb data fetching and updating complete.'));
};

const processNetflix = async (devMode) => {

}



// Main function
export const main = async () => {
    const devMode = false; // if devmode is set to true then it would only take 10 random objects from the data and run the functions on them. 
    await processFirstPass(devMode);
};

main();