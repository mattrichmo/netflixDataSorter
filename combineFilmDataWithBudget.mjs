import fs from 'fs';
import path from 'path';
import readline from 'readline';
import chalk from 'chalk';

// Derive directory name in ES module
const __dirname = path.dirname(new URL(import.meta.url).pathname);

const paths = {
    cleanFilmPath: path.join(__dirname, 'data', 'clean', 'cleanDataFilmCombined.jsonl'),
    theNumbersBudgetDataPath: path.join(__dirname, 'data', 'the-numbers', 'the-numbers-budget-data.jsonl'),
    cleanFilmCombinedPath: path.join(__dirname, 'data', 'clean', 'cleanDataFilmCombined.jsonl'),
};

const combineFilmDataWithBudget = async () => {
    const budgetData = new Map();

    // Read and store budget data
    const budgetStream = fs.createReadStream(paths.theNumbersBudgetDataPath, 'utf8');
    const budgetRL = readline.createInterface({
        input: budgetStream,
        crlfDelay: Infinity,
    });

    for await (const line of budgetRL) {
        const budgetObject = JSON.parse(line);
        const normalizedTitle = budgetObject.title.toLowerCase().replace(/[\W_]+/g, ""); // Normalize titles for comparison
        budgetData.set(normalizedTitle, budgetObject);
    }
    budgetRL.close();

    const filmStream = fs.createReadStream(paths.cleanFilmPath, 'utf8');
    const combinedStream = fs.createWriteStream(paths.cleanFilmCombinedPath, { flags: 'a' });
    const filmRL = readline.createInterface({
        input: filmStream,
        crlfDelay: Infinity,
    });

    for await (const line of filmRL) {
        const filmObject = JSON.parse(line);
        const { coreTitle } = filmObject.meta;
        const normalizedCoreTitle = coreTitle.toLowerCase().replace(/[\W_]+/g, ""); // Normalize for comparison

        if (budgetData.has(normalizedCoreTitle)) {
            const budgetInfo = budgetData.get(normalizedCoreTitle);
            if (!filmObject.data.sources) filmObject.data.sources = {}; // Ensure the sources object exists
            filmObject.data.sources.theNumbers = budgetInfo; // Add or update theNumbers data
            combinedStream.write(`${JSON.stringify(filmObject)}\n`);
        }
    }

    filmRL.close();
    combinedStream.close();

    console.log(chalk.green('Film data combined with budget data successfully.'));
};

combineFilmDataWithBudget().catch(err => {
    console.error(chalk.red('An error occurred:'), err);
});
