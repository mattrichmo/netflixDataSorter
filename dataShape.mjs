

let dataObject = {
    meta: {
        contentUUID: '',
        rawTitle: '', /// the raw title from the jsonl file
        cleanTitle: '', // the title without the // and any text after it and without the year
        coreTitle: '', // the title without : season 1 or : season 2 etc or Part 1 or Part 2, however if it has a : but not season or part after then we should keeop it in
        indexNum: 0, // the original index num from the sortedData.jsonl file
        scrape: {
            status: '', // the status of the scrape
            date: '', // the date of the scrape
            matchConfidence: 0, // the confidence of the match`
            query: '', // the query used to match the title
        }
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

