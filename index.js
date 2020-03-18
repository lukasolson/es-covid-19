const fetch = require('node-fetch');
const parse = require('csv-parse/lib/sync');
const elasticsearch = require('elasticsearch');

const mapping = require('./mapping');
const countries = require('./countries');
const states = require('./states');

const client = new elasticsearch.Client({
	host: 'elastic:changeme@localhost:9200'
});

const index = 'covid-19';

(async function () {
	await client.indices.create({index});
	await client.indices.putMapping({
		index,
		body: mapping
	});
	await insertData();
})();

async function insertData() {
	const confirmedResponse = await fetch(getDataUrl('Confirmed'));
	const deathsResponse = await fetch(getDataUrl('Deaths'));
	const recoveredResponse = await fetch(getDataUrl('Recovered'));

	const confirmedCsv = await confirmedResponse.text();
	const deathsCsv = await deathsResponse.text();
	const recoveredCsv = await recoveredResponse.text();

	const [header, ...confirmedRows] = parse(confirmedCsv);
	const [, ...deathsRows] = parse(deathsCsv);
	const [, ...recoveredRows] = parse(recoveredCsv);

	const [stateKey, countryKey, latKey, longKey, ...dates] = header;
	const offset = [stateKey, countryKey, latKey, longKey].length;

	confirmedRows.forEach(([state, country, lat, lon, ...counts], i) => {
		const docs = counts.map((count, j) => {
			const id = [index, state, country, dates[j]].join('-');
			const timestamp = new Date(dates[j]).toISOString();

			const totalConfirmed = parseFloat(count);
			const totalDeaths = parseFloat(deathsRows[i][j + offset]);
			const totalRecovered = parseFloat(recoveredRows[i][j + offset]);
			const totalActive = totalConfirmed - totalDeaths - totalRecovered;

			const previousConfirmed = j <= 0 ? 0 : parseFloat(counts[j - 1]);
			const previousDeaths = j <= 0 ? 0 : parseFloat(deathsRows[i][j + offset - 1]);
			const previousRecovered = j <= 0 ? 0 : parseFloat(recoveredRows[i][j + offset - 1]);
			const previousActive = previousConfirmed - previousDeaths - previousRecovered;

			const body = {
				'@timestamp': timestamp,
				province_state: getState(state),
				country_region: getCountry(country),
				location: {
					lat: parseFloat(lat),
					lon: parseFloat(lon),
				},
				total_confirmed: totalConfirmed,
				total_deaths: totalDeaths,
				total_recovered: totalRecovered,
				total_active: totalActive,
				new_confirmed: totalConfirmed - previousConfirmed,
				new_deaths: totalDeaths - previousDeaths,
				new_recovered: totalRecovered - previousRecovered,
				new_active: totalActive - previousActive,
				percent_deaths: totalDeaths / totalConfirmed,
				percent_recovered: totalRecovered / totalConfirmed,
			};

			return {index, id, body};
		});

		bulkInsert(docs);
	});
}

function bulkInsert(docs) {
	console.log(`Indexing ${docs.length} docs...`);
	const body = docs.reduce((actions, doc) => actions.concat([
		{index: {_index: doc.index, _id: doc.id}},
		doc.body
	]), []);

	try {
		if (body.length) client.bulk({body});
	} catch (e) {
		console.log(e);
	}
}

function getDataUrl(caseType) {
	return `https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-${caseType}.csv`;
}

/**
 * Fixes some issues with country/region values that don't match up with https://maps.elastic.co/v7.6/index.html?locale=en#file/world_countries
 */
function getCountry(country) {
	return countries.hasOwnProperty(country) ? countries[country] : country;
}

/**
 * Fixes some issues with province/state values that actually include county & state
 * @param countyState
 * @returns {*}
 */
function getState(countyState) {
	const [county, state] = countyState.split(',').map(val => val.trim());
	if (state && states.hasOwnProperty(state)) {
		return states[state];
	}
	return countyState;
}