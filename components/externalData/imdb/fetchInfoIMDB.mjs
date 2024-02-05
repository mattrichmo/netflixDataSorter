import fs from 'fs';
import axios from 'axios';
import cheerio from 'cheerio';
import chalk from 'chalk';
import levenshtein from 'js-levenshtein';
import readline from 'readline';
import { v4 as uuidv4 } from 'uuid';
import path from 'path';




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

            // Enhanced error handling by checking existence before accessing properties
            const titleSelector = $('span.hero__primary-text');
            const title = titleSelector.length > 0 ? titleSelector.text().trim() : 'N/A';
            
            const imdbRatingSelector = $('.sc-69e49b85-1 div.sc-bde20123-0 span.sc-bde20123-1').first();
            const imdbRating = imdbRatingSelector.length > 0 ? imdbRatingSelector.text().trim() : 'N/A';
            
            const numRatingsSelector = $('.sc-69e49b85-1 div.sc-bde20123-3').first();
            const numRatings = numRatingsSelector.length > 0 ? numRatingsSelector.text().trim() : 'N/A';
            
            const outOfSelector = $('.sc-69e49b85-1 div.sc-bde20123-0 span:nth-of-type(2)').first();
            const outOf = outOfSelector.length > 0 ? outOfSelector.text().trim() : 'N/A';
            
            const lengthSelector = $('.sc-d8941411-2 li:nth-of-type(4)');
            const length = lengthSelector.length > 0 ? lengthSelector.text().trim() : 'N/A';
            
            const datesSelector = $('.sc-d8941411-2 li:nth-of-type(2) a');
            const dates = datesSelector.length > 0 ? datesSelector.text().trim() : 'N/A';
            
            const showRatingSelector = $('.sc-d8941411-2 li:nth-of-type(3) a');
            const showRating = showRatingSelector.length > 0 ? showRatingSelector.text().trim() : 'N/A';
            
            const coverSelector = $('.ipc-media--poster-l img');
            const cover = coverSelector.length > 0 ? coverSelector.attr('src').trim() : 'N/A';

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
            const selectors = ['table:nth-of-type(1) tr', 'table:nth-of-type(2) tr', '.cast_list tr', 'table:nth-of-type(4) tr', 'table:nth-of-type(n+5) tr'];

            categories.forEach((category, index) => {
                $(selectors[index]).each((i, elem) => {
                    const nameSelector = $(elem).find('td.name a');
                    const name = nameSelector.length > 0 ? nameSelector.text().trim() : 'N/A';
                    const link = nameSelector.length > 0 ? `https://www.imdb.com${nameSelector.attr('href')}` : 'N/A';
                    const creditDetailsSelector = $(elem).find('td.credit');
                    const creditDetails = creditDetailsSelector.length > 0 ? creditDetailsSelector.text().trim() : 'N/A';
                    
                    if (name !== 'N/A') {
                        const parsedData = parseCreditInfo(creditDetails, categories[index]);
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
export const fetchInfoIMDB = async (query, index) => {
    let status = 'success'; // Initialize status as 'success', will be updated dynamically based on the operation outcomes

    try {
        const searchResult = await searchImdb(query);
        if (!searchResult) {
            console.log(chalk.dim(`----- Index: ${index} -----`));
            status = 'No-Result'; // Update status dynamically
            return { status, index, query };
        }

        const { matchedTitle, imdbLink, releaseDate, type, dates } = searchResult;
        const matchConfidence = calculateMatchConfidence(query, matchedTitle);
        let confidenceColor = matchConfidence < 50 ? chalk.red : matchConfidence < 90 ? chalk.yellow : chalk.green;

        console.log(chalk.dim(`\n-----`));
        console.log(chalk.magenta(`Query: ${query}`));
        console.log(chalk.green(`Matched Title: ${matchedTitle}`));
        console.log(chalk.green(`Release Dates: ${releaseDate}`));
        console.log(chalk.green(`Type: ${type}`));
        console.log(confidenceColor(`Match Confidence: ${matchConfidence}%`));

        if (matchConfidence > 70) {
            console.log(chalk.dim('Fetching Additional General Data for Title:'), chalk.green(matchedTitle), chalk.yellow(imdbLink));
            const generalInfoData = await getImdbGeneralInfo(imdbLink);
            const castCrewData = await getCastCrew(imdbLink);

            return {
                status, // Dynamic status based on the operation outcomes
                data: {
                    matchedTitle: matchedTitle,
                    matchConfidence: parseFloat(matchConfidence),
                    imdbLink: imdbLink,
                    details: {
                        title: generalInfoData.title || matchedTitle,
                        description: generalInfoData.description || '',
                        genres: generalInfoData.genres || [],
                        length: generalInfoData.length || '',
                        dates: {
                            singleDate: dates.singleDate,
                            seriesOngoing: dates.seriesOngoing,
                            seriesHasEnded: dates.seriesHasEnded,
                            singleReleaseDate: dates.singleReleaseDate,
                            startDate: dates.startDate,
                            endDate: dates.endDate
                        },
                        type: type, // Include the type field here
                        contentRating: generalInfoData.contentRating || '',
                        cover: generalInfoData.cover || '',
                        userRating: {
                            rating: generalInfoData.imdbRating || '',
                            numRatings: generalInfoData.numRatings || '',
                            outOf: generalInfoData.outOf || ''
                        },
                    },
                    castCrew: castCrewData
                },
                index,
                query
            };
            
        } else {
            status = 'Low-Confidence'; // Update status dynamically
            console.log(chalk.red('Match confidence Low. Skipping additional scraping.'));
            // Return basic data from the initial search result, even for low-confidence items
            return {
                status,
                data: {
                    matchedTitle: matchedTitle,
                    matchConfidence: parseFloat(matchConfidence),
                    imdbLink: imdbLink,
                    details: {
                        title: matchedTitle,
                        dates: {
                            singleDate: dates.singleDate,
                            seriesOngoing: dates.seriesOngoing,
                            seriesHasEnded: dates.seriesHasEnded,
                            singleReleaseDate: dates.singleReleaseDate,
                            startDate: dates.startDate,
                            endDate: dates.endDate
                        },
                        type: type
                    }
                },
                index,
                query
            };
        }
    } catch (error) {
        status = 'error'; // Update status dynamically in case of an error
        console.error(chalk.red(`Error in fetchInfoIMDB for query: ${query}`), error.message);
        console.log(chalk.dim(`----- Index: ${index} -----`));
        return { 
            status: `${status}-${error.response ? `Axios-Error-${error.response.status}` : 'UnknownError'}`, 
            index, 
            query, 
            retryable: (error.response && error.response.status === 403) || error.code === 'ECONNABORTED'
        };
    }
};






