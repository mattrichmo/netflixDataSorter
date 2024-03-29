import fs from 'fs';
import path from 'path';
import readline from 'readline';
import chalk from 'chalk';
import { createObjectCsvWriter } from 'csv-writer';
import xlsx from 'xlsx';

const inputFilepaths = {
    combined: {
        path: `./data/clean/cleanDataFilmCombined.jsonl`,
    },
    film: {
        path: `./data/clean/cleanDataFilm.jsonl`
    },
    tv: {
        path: `./data/clean/cleanDataTV.jsonl`
    }
};

const outputFilepaths = {
    combined: {
        jsonl: './data/flat/combined/combinedFlat.jsonl',
        csv: './data/flat/combined/combinedFlat.csv',
        xls: './data/flat/combined/combinedFlat.xlsx',
    },
    film: {
        jsonl: '/data/flat/film/filmFlat.jsonl',
        csv: '/data/flat/film/filmFlat.csv',
        xls: '/data/flat/film/filmFlat.xlsx',
    },
    tv: {
        jsonl: '.data/flat/tv/tvFlat.jsonl',
        csv: '.data/flat/tv/tvFlat.csv',
        xls: '.data/flat/tv/tvFlat.xlsx',
    }
};



// Ensure directory exists or create it
const ensureDirectoryExistence = (filePath) => {
    const dirname = path.dirname(filePath);
    if (!fs.existsSync(dirname)) {
        fs.mkdirSync(dirname, { recursive: true });
    }
};

const flattenObject = (obj, type) => {

        // Helper function to safely get the length of an array property or return null
        const getLengthOrNull = (propertyPath) => {
            return propertyPath ? propertyPath.length : null;
        };
    
        // Helper function to safely get a property value or return null
        const getValueOrNull = (propertyPath, joinWith = null) => {
            if (!propertyPath) return null;
            return joinWith ? propertyPath.join(joinWith) : propertyPath;
        };

const headerSets = {
    combined: {
        parentUUID: obj.meta.parentUUID,
        rawTitle: obj.meta.rawTitle,
        cleanTitle: obj.meta.cleanTitle,
        totalHoursWatched: obj.data.totals ? obj.data.totals.totalHoursWatched : null,
        imdbMatchedTitle: getValueOrNull(obj.data.sources.imdb ? obj.data.sources.imdb.matchedTitle : null),
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
        miscellaneousCount: getLengthOrNull(obj.data.sources.imdb && obj.data.sources.imdb.castCrew ? obj.data.sources.imdb.castCrew.miscellaneous : null),
        theNumbersRank: getValueOrNull(obj.data.sources.theNumbers ? obj.data.sources.theNumbers.rank : null),
        theNumbersReleaseDate: getValueOrNull(obj.data.sources.theNumbers ? obj.data.sources.theNumbers.releaseDate : null),
        theNumbersProdBudget: getValueOrNull(obj.data.sources.theNumbers ? obj.data.sources.theNumbers.prodBudget : null),
        theNumbersDomGross: getValueOrNull(obj.data.sources.theNumbers ? obj.data.sources.theNumbers.domGross : null),
        theNumbersWorldGross: getValueOrNull(obj.data.sources.theNumbers ? obj.data.sources.theNumbers.worldGross : null),
    },
    film: {
        parentUUID: obj.meta.parentUUID,
        rawTitle: obj.meta.rawTitle,
        cleanTitle: obj.meta.cleanTitle,
        seasonsInNetflixData: obj.data.totals ? obj.data.totals.seasonsInNetflixData : null,
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
    },
    tv: {
        parentUUID: obj.meta.parentUUID,
        rawTitle: obj.meta.rawTitle,
        cleanTitle: obj.meta.cleanTitle,
        seasonsInNetflixData: obj.data.totals ? obj.data.totals.seasonsInNetflixData : null,
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
    }
}
    const flattened = {};
    const headers = headerSets[type];
    for (const key in headers) {
        flattened[key] = typeof obj[key] !== 'undefined' ? obj[key] : null;
    }
    return flattened;
};

const writeCSVAndXLSX = async (flattenedData, outputPaths, type) => {
    ensureDirectoryExistence(outputPaths.csv);
    const headers = Object.keys(headerSets[type]).map(key => ({id: key, title: key}));
    const csvWriter = createObjectCsvWriter({
        path: outputPaths.csv,
        header: headers
    });

    await csvWriter.writeRecords(flattenedData);
    console.log(chalk.green(`CSV file for ${type} has been written.`));

    ensureDirectoryExistence(outputPaths.xls);
    const workbook = xlsx.utils.book_new();
    const worksheet = xlsx.utils.json_to_sheet(flattenedData, {header: headers.map(h => h.id)});
    xlsx.utils.book_append_sheet(workbook, worksheet, 'Data');
    xlsx.writeFile(workbook, outputPaths.xls);
    console.log(chalk.green(`XLSX file for ${type} has been written.`));
};

export const main = async () => {
    for (const type of Object.keys(inputFilepaths)) {
        console.log(chalk.blue(`Processing ${type} data...`));
        const inputPath = inputFilepaths[type].path;
        const outputPath = outputFilepaths[type];
        ensureDirectoryExistence(outputPath.jsonl);
        const flattenedData = [];
        const jsonlStream = fs.createReadStream(inputPath, 'utf8');
        const rl = readline.createInterface({ input: jsonlStream, crlfDelay: Infinity });
        const jsonlWriteStream = fs.createWriteStream(outputPath.jsonl, { flags: 'a' });

        for await (const line of rl) {
            const obj = JSON.parse(line);
            const flattened = flattenObject(obj, type);
            flattenedData.push(flattened);
            jsonlWriteStream.write(JSON.stringify(flattened) + '\n');
        }

        jsonlWriteStream.close();
        console.log(chalk.green(`${type.toUpperCase()} JSONL file has been written.`));

        await writeCSVAndXLSX(flattenedData, outputPath, type);
    }
};

main().catch(err => {
    console.error(chalk.red('An error occurred:'), err);
});