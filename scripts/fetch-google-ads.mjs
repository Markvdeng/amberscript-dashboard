/**
 * Fetch Google Ads cost data for all Amberscript accounts.
 * Only fetches: campaign name + cost, segmented by week.
 * Parses campaign name for: country, product, user type (Machine-Made/Human-Made).
 *
 * Campaign naming convention:
 *   {Country}_{lang}_SEA_{NB|Brand}_{Product}_{Quality}_{Light|Heavy}_(GA)
 *   e.g. NL_(nl)_SEA_NB_Transcription_Automatic_Light_(GA)
 *
 * Outputs: raw/google-ads.json
 */

import {
  GOOGLE_ADS_ACCOUNTS,
  getDateRange,
  saveRaw,
  retry,
} from './utils.mjs';

const {
  GOOGLE_ADS_DEVELOPER_TOKEN,
  GOOGLE_ADS_CLIENT_ID,
  GOOGLE_ADS_CLIENT_SECRET,
  GOOGLE_ADS_REFRESH_TOKEN,
} = process.env;

const LOGIN_CUSTOMER_ID = '7738847492';

if (!GOOGLE_ADS_DEVELOPER_TOKEN || !GOOGLE_ADS_REFRESH_TOKEN) {
  console.error('Missing Google Ads credentials');
  process.exit(1);
}

async function getAccessToken() {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: GOOGLE_ADS_CLIENT_ID,
      client_secret: GOOGLE_ADS_CLIENT_SECRET,
      refresh_token: GOOGLE_ADS_REFRESH_TOKEN,
    }),
  });
  if (!res.ok) throw new Error(`OAuth failed: ${res.status} ${await res.text()}`);
  return (await res.json()).access_token;
}

async function queryGoogleAds(accessToken, customerId, query) {
  const url = `https://googleads.googleapis.com/v23/customers/${customerId}/googleAds:searchStream`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
      'login-customer-id': LOGIN_CUSTOMER_ID,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`Google Ads API error (${customerId}): ${res.status} ${await res.text()}`);
  const results = await res.json();
  const rows = [];
  for (const batch of results) {
    if (batch.results) rows.push(...batch.results);
  }
  return rows;
}

/**
 * Parse campaign name into structured dimensions.
 *
 * Examples:
 *   NL_(nl)_SEA_NB_Transcription_Automatic_Light_(GA) → { country: NL, product: Transcription, userType: Machine-Made, campaignType: NB }
 *   DE_(de)_SEA_Brand_Amberscript_(GA)                → { country: DE, product: Brand, userType: Brand, campaignType: Brand }
 *   EU_(en)_SEA_NB_AI Meeting Notes_(GA)              → { country: EU, product: AI Meeting Notes, userType: Other, campaignType: NB }
 */
function parseCampaignName(name) {
  const country = (name || '').slice(0, 2).toUpperCase();

  // Detect Brand
  if (/_Brand_/i.test(name)) {
    return { country, product: 'Brand', userType: 'Brand', campaignType: 'Brand' };
  }

  // Detect user type from Light/Heavy in campaign name
  let userType = 'Other';
  if (/_Light[_( ]/i.test(name)) userType = 'Machine-Made';
  if (/_Heavy[_( ]/i.test(name)) userType = 'Human-Made';

  // Extract product from the NB_ part
  // Pattern: _NB_{Product}_{Quality}_{Weight}_\(GA\)
  // Also handles DGN_ prefix: DE_(de)_DGN_NB_Subtitles_Manual_Heavy_(GA)
  const nbMatch = name.match(/_NB_(.+?)_\(GA\)/);
  let product = 'Other';
  if (nbMatch) {
    // Remove experiment comments (after //)
    let raw = nbMatch[1].split('//')[0].trim();
    // Remove quality/weight suffixes
    raw = raw.replace(/_(Automatic|Manual|Light|Heavy)/gi, '');
    // Remove trailing underscores and spaces
    raw = raw.replace(/[_ ]+$/, '');
    product = raw || 'Other';
  }

  return { country, product, userType, campaignType: 'NB' };
}

async function main() {
  const { start, end } = getDateRange();
  console.log(`Fetching Google Ads data: ${start} to ${end}`);

  const accessToken = await getAccessToken();
  const allRows = [];

  const query = `
    SELECT
      campaign.name,
      segments.week,
      metrics.cost_micros
    FROM campaign
    WHERE segments.date BETWEEN '${start}' AND '${end}'
      AND metrics.cost_micros > 0
    ORDER BY segments.week
  `;

  for (const [accountName, customerId] of Object.entries(GOOGLE_ADS_ACCOUNTS)) {
    console.log(`  Fetching ${accountName} (${customerId})...`);
    try {
      const rows = await retry(() => queryGoogleAds(accessToken, customerId, query));
      for (const row of rows) {
        const campaignName = row.campaign?.name || '';
        const parsed = parseCampaignName(campaignName);
        allRows.push({
          account: accountName,
          campaign: campaignName,
          week: row.segments?.week || '',
          cost: (row.metrics?.costMicros || 0) / 1_000_000,
          country: parsed.country,
          product: parsed.product,
          userType: parsed.userType,
          campaignType: parsed.campaignType,
        });
      }
      console.log(`    ${rows.length} rows`);
    } catch (err) {
      console.warn(`    Error fetching ${accountName}: ${err.message}`);
    }
  }

  saveRaw('google-ads.json', allRows);
  console.log(`Done: ${allRows.length} rows`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
