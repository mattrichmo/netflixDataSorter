import fs from 'fs';
import readline from 'readline';
import path from 'path';
import { createReadStream } from 'fs';

async function processIMDBData(inputFile, outputFolder) {
    const genreData = {};

    // Create the output folder if it doesn't exist
    if (!fs.existsSync(outputFolder)) {
        fs.mkdirSync(outputFolder, { recursive: true });
    }

    const fileStream = createReadStream(inputFile);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    for await (const line of rl) {
        const obj = JSON.parse(line);
        const genres = obj.newIMDBData.genres || [];

        genres.forEach(genre => {
            const genreFilePath = path.join(outputFolder, `${genre}Data.jsonl`);
            fs.appendFileSync(genreFilePath, line + '\n');

            if (!genreData[genre]) {
                genreData[genre] = { numItems: 0, AllHoursViewed: 0 };
            }
            genreData[genre].numItems += 1;
            genreData[genre].AllHoursViewed += obj.meta.totalHoursViewed;
        });
    }

    // Create CSV
    let csvContent = 'genre,numItems,AllHoursViewed\n';
    for (const [genre, data] of Object.entries(genreData)) {
        csvContent += `${genre},${data.numItems},${data.AllHoursViewed}\n`;
    }

    fs.writeFileSync('genre_summary.csv', csvContent);
}

// Use the function
const inputFilePath = 'IMDB-DATA-FINAL.jsonl';
const outputFolderPath = 'genreSort';
processIMDBData(inputFilePath, outputFolderPath);
