import fs from 'fs';

import chalk from 'chalk';
import readline from 'readline';
import path from 'path';
import { fetchInfoIMDB } from './components/externalData/imdb/fetchInfoIMDB.mjs';





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
        meta:{
            indexNum: data.meta.indexNum,
            coreTitle: data.meta.coreTitle,
        },

        imdbLink: data.imdbLink // Assuming this is provided in the data passed to manifestWrite
    };
    const stream = fs.createWriteStream(filePath, {flags: 'a'});
    stream.write(`${JSON.stringify(minimalData)}\n`);
    stream.end();
};
const getLastScrapedItem = async () => {
    const filePath = './data/manifest/scrapedManifest.jsonl';
    if (!fs.existsSync(filePath) || fs.statSync(filePath).size === 0) {
        return null; // Return null if the file doesn't exist or is empty
    }

    const data = fs.readFileSync(filePath, 'utf8').trim().split('\n').map(line => {
        const entry = JSON.parse(line);
        entry.meta.indexNum = parseInt(entry.meta.indexNum, 10); // Ensure indexNum is an integer
        return entry;
    });

    if (data.length === 0) {
        return null; // Return null if no data entries were found
    }

    const highestIndexEntry = data.reduce((acc, curr) => (acc.meta.indexNum > curr.meta.indexNum ? acc : curr), data[0]);
    return highestIndexEntry ? {
        indexNum: highestIndexEntry.meta.indexNum
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

// Process each item and handle errors
const processItem = async (item, paths) => {
    try {
        if (!item.meta || !item.meta.coreTitle) {
            throw new Error(`Missing coreTitle for item at index ${item.meta.indexNum}`); // Use meta.indexNum
        }

        // Fetch IMDb information using the item's coreTitle and indexNum
        const result = await fetchInfoIMDB(item.meta.coreTitle, item.meta.indexNum); // Use meta.indexNum

        // Merge fetched IMDb data into the existing data structure, under sources.imdb
        const updatedItem = {
            ...item, // Spread the original item to keep existing fields
            data: {
                ...item.data, // Spread existing data fields
                sources: {
                    ...item.data.sources, // Spread existing sources
                    imdb: result.data // Assign the fetched IMDb data to the imdb source
                }
            }
        };

        // Generate minimal data structure for append
        const minimalData = {
            status: result.status,
            indexNum: item.meta.indexNum, // Use meta.indexNum
            coreTitle: item.meta.coreTitle,
            imdbLink: result.data ? result.data.imdbLink : null,
            matchedTitle: result.data ? result.data.matchedTitle : '',
            matchConfidence: result.data ? result.data.matchConfidence : 0,
            releaseDate: result.data ? result.data.details.dates : {},
            type: result.data ? result.data.type : '',
        };

        // Append the updated item with IMDb data to the output file
        fs.appendFileSync(paths.outputFilePath, `${JSON.stringify(updatedItem)}\n`);
        // Append the minimal data structure for any other required operations
        fs.appendFileSync(paths.minOutputFilePath, `${JSON.stringify(minimalData)}\n`);

        // Update the manifest as before, no changes required here
        await manifestWrite({
            status: result.status,
            meta: {
                indexNum: item.meta.indexNum,
                coreTitle: item.meta.coreTitle
            },
            imdbLink: result.data ? result.data.imdbLink : null,
            matchConfidence: result.data ? result.data.matchConfidence : null
        });
    } catch (error) {
        console.error(chalk.red(`Error processing item at index ${item.meta.indexNum}: ${error.message}`));
        const errorData = {
            indexNum: item.meta.indexNum,
            coreTitle: item.meta.coreTitle,
            error: {
                message: error.message,
                stack: error.stack
            }
        };
        // Append error data to errorData.jsonl file
        fs.appendFileSync(paths.errorFilePath, `${JSON.stringify(errorData)}\n`);
    }
};

const processBatch = async (batch, paths) => {
    const errorFilePath = './data/errors/errorData.jsonl'; // Assuming this is defined in your paths
    const maxRetries = 3;
    const retryDelay = 4000; // Delay in milliseconds
    const concurrentRequests = 10;

    // Function to process a single item with retries
    const processWithRetries = async (item) => {
        let retryCount = 0;
        while (retryCount <= maxRetries) {
            try {
                await processItem(item, paths);
                return { success: true }; // Exit the loop on success
            } catch (error) {
                console.error(chalk.yellow(`Error processing item at index ${item.meta.indexNum}. Retry attempt ${retryCount + 1}`), error);
                if (retryCount < maxRetries) {
                    await sleep(retryDelay);
                    retryCount++;
                } else {
                    // Enhanced error handling to capture and log detailed error information
                    console.error(chalk.red(`Failed after ${maxRetries} retries for item at index ${item.meta.indexNum}:`), error);
                    const detailedError = {
                        item,
                        error: {
                            message: error.message,
                            stack: error.stack, // Capture the stack trace for more detailed debugging
                            type: error.name // Capture the error type (e.g., TypeError)
                        },
                        status: 'error',
                        retryCount: maxRetries
                    };
                    fs.appendFileSync(errorFilePath, `${JSON.stringify(detailedError)}\n`);
                    return { success: false, error: `Failed after ${maxRetries} retries for item at index ${item.meta.indexNum}: ${error.message}` };
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
const processAllBatches = async (paths) => {
    const batchSize = 30;
    const batchDelay = 3000; // 3 seconds
    
    const sortedDataFilePath = './data/processed/sortedData.jsonl';

    // Function to generate a random delay between 1 and 3 seconds
    const randomDelay = () => Math.floor(Math.random() * (batchDelay - 1000 + 1)) + 1000;

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
        const currentIndex = item.meta.indexNum;

        if (currentIndex >= startIndex) {
            currentBatch.push(item);
            if (currentBatch.length === 1) {
                firstIndexInBatch = currentIndex; // Update firstIndexInBatch at the start of a new batch
            }

            if (currentBatch.length === batchSize || rl.readableEnded) {
                const batchEndIndex = currentBatch.length < batchSize ? firstIndexInBatch : firstIndexInBatch + currentBatch.length - 1;
                console.log(chalk.magenta(`Processing Items (${firstIndexInBatch} - ${batchEndIndex}) of total ${totalItems}.`));

                await processBatch(currentBatch, paths);
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
    await processAllBatches(paths);
    console.log('Data processing completed.');
};

main();
