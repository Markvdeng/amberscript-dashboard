/**
 * Fetch GA4 form submissions (generate_lead) and purchases.
 * Uses separate OAuth token (marketing@amberscript.com) for GA4 access.
 * Outputs: raw/ga4.json
 */

import { google } from 'googleapis';
import { getDateRange, getMonth, saveRaw, retry } from './utils.mjs';

const GA4_PROPERTY = process.env.GA4_PROPERTY_ID || 'properties/261585855';

const {
  GOOGLE_ADS_CLIENT_ID,
  GOOGLE_ADS_CLIENT_SECRET,
  GA4_REFRESH_TOKEN,
} = process.env;

if (!GA4_REFRESH_TOKEN) {
  console.error('Missing GA4_REFRESH_TOKEN');
  process.exit(1);
}

async function getClient() {
  const oauth2Client = new google.auth.OAuth2(GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET);
  oauth2Client.setCredentials({ refresh_token: GA4_REFRESH_TOKEN });
  return google.analyticsdata({ version: 'v1beta', auth: oauth2Client });
}

/**
 * Convert ISO year+week (e.g., "202607") to Monday date string
 */
function isoWeekToDate(isoWeek) {
  if (!isoWeek || isoWeek.length < 6) return '';
  const year = parseInt(isoWeek.slice(0, 4));
  const week = parseInt(isoWeek.slice(4));
  const jan4 = new Date(Date.UTC(year, 0, 4));
  const dayOfWeek = jan4.getUTCDay() || 7;
  const monday = new Date(jan4);
  monday.setUTCDate(jan4.getUTCDate() - dayOfWeek + 1 + (week - 1) * 7);
  return monday.toISOString().slice(0, 10);
}

async function fetchAll(client, requestBody) {
  const rows = [];
  let offset = 0;
  const limit = 10000;

  while (true) {
    const res = await retry(() => client.properties.runReport({
      property: GA4_PROPERTY,
      requestBody: { ...requestBody, offset, limit },
    }));

    const data = res.data;
    if (!data.rows || data.rows.length === 0) break;
    rows.push(...data.rows);
    offset += data.rows.length;
    if (data.rows.length < limit) break;
    console.log(`    ${rows.length} rows so far...`);
  }

  return rows;
}

async function main() {
  const { start, end } = getDateRange();
  console.log(`Fetching GA4 data: ${start} to ${end}`);

  const client = await getClient();

  // --- Form Submissions (generate_lead) ---
  console.log('  Fetching generate_lead events...');
  const leadRows = await fetchAll(client, {
    dateRanges: [{ startDate: start, endDate: end }],
    dimensions: [
      { name: 'isoYearIsoWeek' },
      { name: 'countryId' },
      { name: 'customEvent:form_name' },
      { name: 'customEvent:form_id_long' },
      { name: 'customEvent:form_product' },
      { name: 'firstUserDefaultChannelGroup' },
    ],
    metrics: [{ name: 'eventCount' }],
    dimensionFilter: {
      filter: { fieldName: 'eventName', stringFilter: { matchType: 'EXACT', value: 'generate_lead' } },
    },
  });
  console.log(`    ${leadRows.length} rows`);

  const formSubmissions = leadRows.map(row => {
    const d = row.dimensionValues;
    const weekDate = isoWeekToDate(d[0].value);
    const formName = d[2].value || '';
    const formIdLong = d[3].value || '';
    return {
      week: weekDate,
      month: weekDate ? getMonth(weekDate) : '',
      country: d[1].value || '',
      formName,
      formIdLong,
      formId: formName && formIdLong ? `${formName}_${formIdLong}` : '',
      product: d[4].value || '',
      channel: d[5].value || '',
      count: parseInt(row.metricValues[0].value || 0),
    };
  });

  // --- Purchases ---
  console.log('  Fetching purchase events...');
  const purchaseRows = await fetchAll(client, {
    dateRanges: [{ startDate: start, endDate: end }],
    dimensions: [
      { name: 'isoYearIsoWeek' },
      { name: 'transactionId' },
      { name: 'sessionDefaultChannelGroup' },
      { name: 'sessionCampaignName' },
    ],
    metrics: [
      { name: 'transactions' },
      { name: 'purchaseRevenue' },
    ],
    dimensionFilter: {
      filter: { fieldName: 'eventName', stringFilter: { matchType: 'EXACT', value: 'purchase' } },
    },
  });
  console.log(`    ${purchaseRows.length} rows`);

  const purchases = purchaseRows.map(row => {
    const d = row.dimensionValues;
    const m = row.metricValues;
    const weekDate = isoWeekToDate(d[0].value);
    return {
      week: weekDate,
      month: weekDate ? getMonth(weekDate) : '',
      transactionId: d[1].value || '',
      channel: d[2].value || '',
      campaign: d[3].value || '',
      transactions: parseInt(m[0].value || 0),
      revenue: parseFloat(m[1].value || 0),
    };
  });

  const output = { formSubmissions, purchases };
  saveRaw('ga4.json', output);
  console.log(`Done: ${formSubmissions.length} form submissions, ${purchases.length} purchases`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
