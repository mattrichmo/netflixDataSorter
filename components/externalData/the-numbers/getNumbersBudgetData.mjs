import fs from 'fs';
import axios from 'axios';
import cheerio from 'cheerio';
import chalk from 'chalk';
import levenshtein from 'js-levenshtein';
import readline from 'readline';
import { v4 as uuidv4 } from 'uuid';
import path from 'path';

const baseUrl = `https://www.the-numbers.com/movie/budgets/all`;
const outputFilePath = `../data/the-numbers/the-numbers-budget-data.jsonl`;

// Ensure the output directory exists
const outputDir = path.dirname(outputFilePath);
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

let allNumberObjects = [];

const fetchPageData = async (url) => {
  try {
    const response = await axios.get(url);
    return response.data;
  } catch (error) {
    console.error(chalk.red(`Failed to fetch data from ${url}: ${error}`));
    return null;
  }
};

const parseHtml = (html) => {
  const $ = cheerio.load(html);
  $('tbody tr').each((index, element) => {
    const rank = $(element).find('td:nth-of-type(1)').text().trim();
    const releaseDate = $(element).find('td > a').text().trim();
    const titleUrl = $(element).find('b a').attr('href');
    const prodBudget = $(element).find('td:nth-of-type(4)').text().trim();
    const domGross = $(element).find('td:nth-of-type(5)').text().trim();
    const worldGross = $(element).find('td:nth-of-type(6)').text().trim();
    const titleText = $(element).find('b a').text().trim();

    const numbersDataObject = {
      rowUUID: uuidv4(),
      rank,
      releaseDate,
      title: titleText,
      titleUrl: `https://www.the-numbers.com${titleUrl}`,
      prodBudget,
      domGross,
      worldGross,
    };

    allNumberObjects.push(numbersDataObject);
  });
};

const appendToFile = (data) => {
  const dataString = data.map(obj => JSON.stringify(obj)).join('\n');
  fs.appendFileSync(outputFilePath, dataString + '\n', 'utf8');
};

const main = async () => {
  let currentPage = 1;
  let hasData = true;

  while (hasData) {
    const currentUrl = currentPage === 1 ? baseUrl : `${baseUrl}/${(currentPage - 1) * 500 + 1}`;
    console.log(chalk.green(`Fetching data from: ${currentUrl}`));
    const html = await fetchPageData(currentUrl);
    if (html) {
      parseHtml(html);
      if (allNumberObjects.length > 0) {
        appendToFile(allNumberObjects);
        allNumberObjects = []; // Reset for the next batch
        currentPage++;
      } else {
        hasData = false;
      }
    } else {
      hasData = false;
    }
  }

  console.log(chalk.blue('Scraping complete.'));
};

main();
