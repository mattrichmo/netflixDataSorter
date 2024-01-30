
import axios from 'axios';
import cheerio from 'cheerio';

let dataShape = {
    flags: {
    },
    meta: {},
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

};


async function getIMDBCastCrew(fullCreditsUrl, headers) {
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
}

function parseCreditInfo(creditString, defaultRole = '') {
    const roleRegex = /^(.*?)\s*\(/;
    const episodesRegex = /(\d+)\s*episodes/;
    const yearsRegex = /(\d{4})-(\d{4}|\s*)/;

    const roleMatch = roleRegex.exec(creditString);
    const role = roleMatch ? roleMatch[1] : defaultRole;

    const episodesMatch = episodesRegex.exec(creditString);
    const numEpisodes = episodesMatch ? episodesMatch[1] : '';

    const yearsMatch = yearsRegex.exec(creditString);
    const dateStart = yearsMatch ? yearsMatch[1] : '';
    const dateEnd = yearsMatch && yearsMatch[2].trim() ? yearsMatch[2] : '';

    return {
        role,
        numEpisodes,
        date: {
            dateStart,
            dateEnd,
            numYears: dateEnd ? (parseInt(dateEnd) - parseInt(dateStart)).toString() : ''
        }
    };
}

async function scrapeIMDbData() {
    const baseUrl = 'https://www.imdb.com/title/tt0413573/';
    const fullCreditsUrl = `${baseUrl}fullcredits`;
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    };

    try {
        // Scrape main IMDb page
        const response = await axios.get(baseUrl, { headers });
        const html = response.data;
        const $ = cheerio.load(html);

        // Extract main page data
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

        // Get cast and crew data
        const castCrew = await getIMDBCastCrew(fullCreditsUrl, headers);

        // Construct the final object
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
            castCrew
        };

        // Log the final data as JSON string
        console.log(JSON.stringify(imdbObject, null, 2));
    } catch (error) {
        console.error('Error scraping IMDb data:', error);
    }
}

scrapeIMDbData();
