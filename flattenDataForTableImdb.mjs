import fs from 'fs';
import path from 'path';
import readline from 'readline';
import chalk from 'chalk';
import { createObjectCsvWriter } from 'csv-writer';
import xlsx from 'xlsx';

const inputPath = './data/clean/cleanDataFilm.jsonl';

const outputFilePath = {
    jsonl: './data/flat/cleanDataFilmFlat.jsonl',
    csv: './data/flat/cleanDataFilmFlat.csv',
    xls: './data/flat/cleanDataFilmFlat.xlsx',
};

// Ensure directory exists or create it
const ensureDirectoryExistence = (filePath) => {
    const dirname = path.dirname(filePath);
    if (!fs.existsSync(dirname)) {
        fs.mkdirSync(dirname, { recursive: true });
    }
};

const flattenObject = (obj) => {
    // Helper function to safely get the length of an array property or return null
    const getLengthOrNull = (propertyPath) => {
        return propertyPath ? propertyPath.length : null;
    };

    // Helper function to safely get a property value or return null
    const getValueOrNull = (propertyPath, joinWith = null) => {
        if (!propertyPath) return null;
        return joinWith ? propertyPath.join(joinWith) : propertyPath;
    };

    const flattened = {
        parentUUID: obj.meta.parentUUID,
        rawTitle: obj.meta.rawTitle,
        cleanTitle: obj.meta.coreTitle,
        secondaryLanguageTitle: obj.meta.secondaryLanguageTitle ? obj.meta.secondaryLanguageTitle: null,
        totalHoursWatched: obj.data.totals ? obj.data.totals.totalHoursWatched : null,
        imdbMatchedTitle: getValueOrNull(obj.data.sources.imdb ? obj.data.sources.imdb.matchedTitle : null),
        imdbMatchConfidence: getValueOrNull(obj.data.sources.imdb ? obj.data.sources.imdb.matchConfidence : null),
        imdbLink: getValueOrNull(obj.data.sources.imdb ? obj.data.sources.imdb.imdbLink : null),
        imdbGenres: getValueOrNull(obj.data.sources.imdb && obj.data.sources.imdb.details.genres, ', '),
        imdbType: getValueOrNull(obj.data.sources.imdb ? obj.data.sources.imdb.details.type : null),
        imdbUserRating: getValueOrNull(obj.data.sources.imdb && obj.data.sources.imdb.details.userRating ? obj.data.sources.imdb.details.userRating.rating : null),
        imdbNumRatings: getValueOrNull(obj.data.sources.imdb && obj.data.sources.imdb.details.userRating ? obj.data.sources.imdb.details.userRating.numRatings : null),
        directorCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.director : null),
        writerCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.writer : null),
        castCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.cast : null),
        producerCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.producer : null),
        composerCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.composer : null),
        cinematographerCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.cinematographer : null),
        editorCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.editor : null),
        castingDirectorCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.casting_director : null),
        artDirectorCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.art_director : null),
        setDecoratorCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.set_decorator : null),
        costumeDesignerCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.costume_designer : null),
        makeUpDepartmentCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.make_up_department : null),
        productionManagerCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.production_manager : null),
        assistantDirectorCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.assistant_director : null),
        artDepartmentCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.art_department : null),
        soundDepartmentCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.sound_department : null),
        stuntsCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.stunts : null),
        cameraDepartmentCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.camera_department : null),
        costumeDepartmentCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.costume_department : null),
        editorialDepartmentCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.editorial_department : null),
        musicDepartmentCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.music_department : null),
        scriptDepartmentCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.script_department : null),
        miscellaneousCrewCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.miscellaneous : null),
    };
    return flattened;
};


const writeCSVAndXLSX = async (flattenedData) => {
    ensureDirectoryExistence(outputFilePath.csv);
    const csvWriter = createObjectCsvWriter({
        path: outputFilePath.csv,
        header: Object.keys(flattenedData[0]).map(key => ({id: key, title: key})),
    });

    await csvWriter.writeRecords(flattenedData);
    console.log(chalk.green('CSV file has been written.'));

    ensureDirectoryExistence(outputFilePath.xls);
    const workbook = xlsx.utils.book_new();
    const worksheet = xlsx.utils.json_to_sheet(flattenedData);
    xlsx.utils.book_append_sheet(workbook, worksheet, 'Data');
    xlsx.writeFile(workbook, outputFilePath.xls);
    console.log(chalk.green('XLSX file has been written.'));
};

export const main = async () => {
    ensureDirectoryExistence(outputFilePath.jsonl);
    const flattenedData = [];
    const jsonlStream = fs.createReadStream(inputPath, 'utf8');
    const rl = readline.createInterface({
        input: jsonlStream,
        crlfDelay: Infinity,
    });

    const jsonlWriteStream = fs.createWriteStream(outputFilePath.jsonl, { flags: 'a' });

    for await (const line of rl) {
        const obj = JSON.parse(line);
        const flattened = flattenObject(obj);
        flattenedData.push(flattened);
        jsonlWriteStream.write(JSON.stringify(flattened) + '\n');
    }

    jsonlWriteStream.close();
    console.log(chalk.green('JSONL file has been written.'));

    await writeCSVAndXLSX(flattenedData);
};

main().catch(err => {
    console.error(chalk.red('An error occurred:'), err);
});
