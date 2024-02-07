import fs from 'fs';
import path from 'path';
import readline from 'readline';
import chalk from 'chalk';

// Derive directory name in ES module
const __dirname = path.dirname(new URL(import.meta.url).pathname);

const paths = {
    inputPath: path.join(__dirname, 'data', 'processed', 'scrapedData.jsonl'),
    cleanPath: path.join(__dirname, 'data', 'clean', 'cleanData.jsonl'),
    lowConfidencePath: path.join(__dirname, 'data', 'clean', 'lowConfidenceData.jsonl'),
    errorPath: path.join(__dirname, 'data', 'errors', 'errorData.jsonl'),
    cleanFilmPath: path.join(__dirname, 'data', 'clean', 'cleanDataFilm.jsonl'),
    cleanTVPath: path.join(__dirname, 'data', 'clean', 'cleanDataTV.jsonl'),
    cleanOtherPath: path.join(__dirname, 'data', 'clean', 'cleanDataOther.jsonl'),
};

// Counters for each category
const counters = {
    clean: 0,
    lowConfidence: 0,
    error: 0,
    film: 0,
    tv: 0,
    other: 0,
};

const createPaths = async () => {
    const dirs = Object.values(paths).map(path.dirname);
    const uniqueDirs = [...new Set(dirs)]; // Ensure no duplicates
    for (const dir of uniqueDirs) {
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
            console.log(chalk.green(`Created directory: ${dir}`));
        }
    }
};

const processTitle = (title) => {
    let cleanTitle = title.split('//')[0].trim();
    cleanTitle = cleanTitle.replace(/\s+\(\d{4}\)$/, '');
    let coreTitle = cleanTitle.split(':').length > 1 && !/season \d+|part \d+/i.test(cleanTitle.split(':')[1]) ? cleanTitle : cleanTitle.split(':')[0];
    return { cleanTitle, coreTitle };
};

const writeToStream = (stream, data, category) => {
    stream.write(`${JSON.stringify(data)}\n`);
    counters[category]++; // Increment the appropriate counter
    updateConsole(); // Update the console display
};

// Refactored to reuse streams instead of opening new ones every time
const categorizeAndCopy = (dataObject, cleanFilmStream, cleanTVStream, cleanOtherStream) => {
    const type = dataObject.data?.sources?.imdb?.details?.type || '';
    if (type === 'Film') {
        writeToStream(cleanFilmStream, dataObject, 'film');
    } else if (type.includes('TV')) {
        writeToStream(cleanTVStream, dataObject, 'tv');
    } else {
        writeToStream(cleanOtherStream, dataObject, 'other');
    }
};

const updateConsole = () => {
    process.stdout.write(`\r${chalk.blue('Processing:')} Clean: ${counters.clean}, Low Confidence: ${counters.lowConfidence}, Errors: ${counters.error}, Films: ${counters.film}, TV: ${counters.tv}, Other: ${counters.other}`);
};

export const main = async () => {
    await createPaths();
    console.log(chalk.blue('Starting processing...')); // Initial log

    const inputStream = fs.createReadStream(paths.inputPath, 'utf8');
    const cleanStream = fs.createWriteStream(paths.cleanPath, { flags: 'a' });
    const lowConfidenceStream = fs.createWriteStream(paths.lowConfidencePath, { flags: 'a' });
    const errorStream = fs.createWriteStream(paths.errorPath, { flags: 'a' });
    // Open streams for film, TV, and other categories
    const cleanFilmStream = fs.createWriteStream(paths.cleanFilmPath, { flags: 'a' });
    const cleanTVStream = fs.createWriteStream(paths.cleanTVPath, { flags: 'a' });
    const cleanOtherStream = fs.createWriteStream(paths.cleanOtherPath, { flags: 'a' });

    const rl = readline.createInterface({
        input: inputStream,
        crlfDelay: Infinity,
    });

    for await (const line of rl) {
        try {
            const dataObject = JSON.parse(line);
            const { cleanTitle, coreTitle } = processTitle(dataObject.meta.rawTitle);
            dataObject.meta.cleanTitle = cleanTitle;
            dataObject.meta.coreTitle = coreTitle;

            const confidenceLevel = dataObject.data?.sources?.imdb?.matchConfidence;

            if (confidenceLevel !== undefined) {
                if (confidenceLevel >= 80) {
                    writeToStream(cleanStream, dataObject, 'clean');
                    categorizeAndCopy(dataObject, cleanFilmStream, cleanTVStream, cleanOtherStream);
                } else {
                    writeToStream(lowConfidenceStream, dataObject, 'lowConfidence');
                }
            } else {
                throw new Error('Confidence level is undefined');
            }
        } catch (error) {
            let errorData;
            try {
                errorData = JSON.parse(line);
            } catch {
                errorData = { originalData: line };
            }
            errorData.error = error.message;
            writeToStream(errorStream, errorData, 'error');
        }
    }

    // Close all streams after processing is complete
    cleanStream.close();
    lowConfidenceStream.close();
    errorStream.close();
    cleanFilmStream.close();
    cleanTVStream.close();
    cleanOtherStream.close();
    console.log(chalk.green('\nProcessing completed.'));
};

main();
