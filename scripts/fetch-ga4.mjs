/**
 * Fetch GA4 form submissions (generate_lead) and purchases.
 * Uses Google Analytics Data API v1beta with OAuth (same creds as Google Ads).
 * Outputs: raw/ga4.json
 */

import { google } from 'googleapis';
import { getDateRange, classifyChannel, classifyGeo, saveRaw, retry } from './utils.mjs';

// GA4 property for Amberscript (measurement ID G-FR5QG4NGRG)
// You can find the numeric property ID in GA4 Admin > Property Settings
const GA4_PROPERTY = process.env.GA4_PROPERTY_ID || '';

const {
  GOOGLE_ADS_CLIENT_ID,
  GOOGLE_ADS_CLIENT_SECRET,
  GOOGLE_ADS_REFRESH_TOKEN,
} = process.env;

if (!GOOGLE_ADS_REFRESH_TOKEN) {
  console.error('Missing Google OAuth credentials (GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN)');
  process.exit(1);
}

if (!GA4_PROPERTY) {
  console.error('Missing GA4_PROPERTY_ID (e.g., "properties/123456789"). Find it in GA4 Admin > Property Settings.');
  process.exit(1);
}

async function getAnalyticsClient() {
  const oauth2Client = new google.auth.OAuth2(
    GOOGLE_ADS_CLIENT_ID,
    GOOGLE_ADS_CLIENT_SECRET,
  );
  oauth2Client.setCredentials({ refresh_token: GOOGLE_ADS_REFRESH_TOKEN });
  return google.analyticsdata({ version: 'v1beta', auth: oauth2Client });
}

async function runReport(client, propertyId, eventName, startDate, endDate) {
  const rows = [];
  let offset = 0;
  const limit = 10000;

  while (true) {
    const res = await retry(async () => {
      const response = await client.properties.runReport({
        property: propertyId,
        requestBody: {
          dateRanges: [{ startDate, endDate }],
          dimensions: [
            { name: 'eventName' },
            { name: 'sessionSourceMedium' },
            { name: 'country' },
            { name: 'isoYearIsoWeek' },
          ],
          metrics: [
            { name: 'eventCount' },
            { name: 'eventValue' },
          ],
          dimensionFilter: {
            filter: {
              fieldName: 'eventName',
              stringFilter: { matchType: 'EXACT', value: eventName },
            },
          },
          offset,
          limit,
        },
      });
      return response;
    });

    const data = res.data;
    if (!data.rows || data.rows.length === 0) break;

    for (const row of data.rows) {
      const dims = row.dimensionValues || [];
      const metrics = row.metricValues || [];
      const sourceMedium = dims[1]?.value || '';
      const [source, medium] = sourceMedium.split(' / ');

      rows.push({
        eventName: dims[0]?.value || '',
        source: source || '',
        medium: medium || '',
        country: dims[2]?.value || '',
        isoWeek: dims[3]?.value || '',
        count: parseInt(metrics[0]?.value || 0),
        value: parseFloat(metrics[1]?.value || 0),
      });
    }

    offset += data.rows.length;
    if (data.rows.length < limit) break;
  }

  return rows;
}

/**
 * Convert ISO year+week (e.g., "202601") to a Monday date string
 */
function isoWeekToDate(isoWeek) {
  if (!isoWeek || isoWeek.length < 6) return '';
  const year = parseInt(isoWeek.slice(0, 4));
  const week = parseInt(isoWeek.slice(4));
  // Jan 4th is always in week 1
  const jan4 = new Date(Date.UTC(year, 0, 4));
  const dayOfWeek = jan4.getUTCDay() || 7;
  const monday = new Date(jan4);
  monday.setUTCDate(jan4.getUTCDate() - dayOfWeek + 1 + (week - 1) * 7);
  return monday.toISOString().slice(0, 10);
}

async function main() {
  const { start, end } = getDateRange(365);
  console.log(`Fetching GA4 data: ${start} to ${end}`);

  const client = await getAnalyticsClient();

  console.log('  Fetching generate_lead events...');
  const leads = await runReport(client, GA4_PROPERTY, 'generate_lead', start, end);
  console.log(`    ${leads.length} rows`);

  console.log('  Fetching purchase events...');
  const purchases = await runReport(client, GA4_PROPERTY, 'purchase', start, end);
  console.log(`    ${purchases.length} rows`);

  const output = {
    formSubmissions: leads.map(r => ({
      week: isoWeekToDate(r.isoWeek),
      channel: classifyChannel(r.source, r.medium),
      geo: classifyGeo(r.country),
      count: r.count,
      source: r.source,
      medium: r.medium,
    })),
    purchases: purchases.map(r => ({
      week: isoWeekToDate(r.isoWeek),
      channel: classifyChannel(r.source, r.medium),
      geo: classifyGeo(r.country),
      count: r.count,
      value: r.value,
      source: r.source,
      medium: r.medium,
    })),
  };

  saveRaw('ga4.json', output);
  console.log(`Done: ${output.formSubmissions.length} form submissions, ${output.purchases.length} purchases`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
