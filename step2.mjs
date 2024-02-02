import fs from 'fs';
import axios from 'axios';
import cheerio from 'cheerio';
import chalk from 'chalk';
import levenshtein from 'js-levenshtein';

// sample data object
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

const getCastCrew = async (imdbLink) => {
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

        // Directors
        $('table:nth-of-type(1) tr').each((i, elem) => {
            const name = $(elem).find('td.name a').text().trim();
            const imdbLink = $(elem).find('td.name a').attr('href');
            const episodesAndYears = $(elem).find('td.credit').text().trim();
            if (name) {
                const parsedData = parseCreditInfo(episodesAndYears, 'director');
                castCrew.directors.push({ name, imdbLink, ...parsedData });
            }
        });

        // Writers
        $('table:nth-of-type(2) tbody tr').each((i, elem) => {
            const name = $(elem).find('td.name a').text().trim();
            const imdbLink = $(elem).find('td.name a').attr('href');
            const titleAndEpisodesAndYears = $(elem).find('td.credit').text().trim();
            if (name) {
                const parsedData = parseCreditInfo(titleAndEpisodesAndYears, 'writer');
                castCrew.writers.push({ name, imdbLink, ...parsedData });
            }
        });

        // Cast
        $('.cast_list tbody tr:nth-of-type(n+2)').each((i, elem) => {
            const name = $(elem).find('td:nth-of-type(2) a').text().trim();
            const imdbLink = $(elem).find('td:nth-of-type(2) a').attr('href');
            const characterName = $(elem).find('.character a:nth-of-type(1)').text().trim();
            const numEpisodes = $(elem).find('a.toggle-episodes').text().trim();
            if (name) {
                const parsedData = parseCreditInfo(numEpisodes, 'actor');
                castCrew.actors.push({ name, imdbLink, role: 'actor', characterName, ...parsedData });
            }
        });

        // Producers
        $('table:nth-of-type(4) tr').each((i, elem) => {
            const name = $(elem).find('td.name a').text().trim();
            const imdbLink = $(elem).find('td.name a').attr('href');
            const titleAndEpisodes = $(elem).find('td.credit').text().trim();
            if (name) {
                const parsedData = parseCreditInfo(titleAndEpisodes);
                castCrew.producers.push({ name, imdbLink, ...parsedData });
            }
        });

        // Crew
        $('table:nth-of-type(n+5) tr').each((i, elem) => {
            const name = $(elem).find('td.name a').text().trim();
            const imdbLink = $(elem).find('td.name a').attr('href');
            const titleAndEpisodes = $(elem).find('td.credit').text().trim();
            if (name) {
                const parsedData = parseCreditInfo(titleAndEpisodes);
                castCrew.crew.push({ name, imdbLink, ...parsedData });
            }
        });

        return castCrew;
    } catch (error) {
        console.error('Error scraping IMDb cast and crew data:', error);
        return null;
    }
};


const getImdbGeneralInfo = async (imdbLink) => {
    const imdbPage = await axios.get(imdbLink, { headers, timeout: 10000 });
    const $ = cheerio.load(imdbPage.data);

    // Extract IMDb data using the provided selectors
    const title = $('span.hero__primary-text').text();
    const imdbRatingElements = $('.sc-69e49b85-1 div.sc-bde20123-0');
    const ratings = imdbRatingElements.map((i, el) => {
        const ratingElement = $(el);
        const rating = ratingElement.find('span.sc-bde20123-1').text();
        const numRatings = ratingElement.find('div.sc-bde20123-3').text();
        const outOf = ratingElement.find('span:nth-of-type(2)').text();
        return { rating, numRatings, outOf };
    }).get();

    const length = $('.sc-d8941411-2 li:nth-of-type(4)').text();
    const dates = $('.sc-d8941411-2 li:nth-of-type(2) a').text();
    const showRating = $('.sc-d8941411-2 li:nth-of-type(3) a').text();
    const cover = $('.ipc-media--poster-l img').attr('src');

    // Update selectors for storyline, genres, and keywords
    const storyline = $('.sc-9eebdf80-1 div').text().trim();

    // New genre selector implementation
    let genres = [];
    $('.ipc-chip-list--baseAlt div.ipc-chip-list__scroller span').each((i, element) => {
        genres.push($(element).text().trim());
    });


};

const processItem = async (imdbLink) => {

    await getImdbGeneralInfo(imdbLink);
    await getCastCrew(imdbLink);
    

};

const processBatch = async () => {}

const main = async () => {

};

main();