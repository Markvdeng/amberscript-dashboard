/**
 * Fetch HubSpot deals with contact associations for Amberscript.
 * Uses HubSpot CRM API v3.
 * Outputs: raw/hubspot-deals.json
 */

import { getDateRange, getWeekStart, classifyChannel, classifyGeo, saveRaw, retry } from './utils.mjs';

const TOKEN = process.env.HUBSPOT_AMBERSCRIPT_TOKEN;
if (!TOKEN) {
  console.error('Missing HUBSPOT_AMBERSCRIPT_TOKEN');
  process.exit(1);
}

const HS_BASE = 'https://api.hubapi.com';

async function hsGet(path, params = {}) {
  const url = new URL(`${HS_BASE}${path}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) url.searchParams.set(k, v);
  }
  const res = await fetch(url, {
    headers: { 'Authorization': `Bearer ${TOKEN}` },
  });
  if (!res.ok) throw new Error(`HubSpot API error: ${res.status} ${await res.text()}`);
  return res.json();
}

async function hsPost(path, body) {
  const res = await fetch(`${HS_BASE}${path}`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`HubSpot API error: ${res.status} ${await res.text()}`);
  return res.json();
}

// Deal stage mapping (customize based on Amberscript's pipeline)
const STAGE_MAP = {
  // Common HubSpot default stages
  'appointmentscheduled': 'lead',
  'qualifiedtobuy': 'MQL',
  'presentationscheduled': 'SQL',
  'decisionmakerboughtin': 'SQL',
  'contractsent': 'SQL',
  'closedwon': 'closed-won',
  'closedlost': 'closed-lost',
};

function mapStage(stageId) {
  return STAGE_MAP[stageId] || stageId;
}

async function fetchAllDeals(since) {
  const deals = [];
  let after = undefined;

  const properties = [
    'dealname', 'dealstage', 'amount', 'closedate', 'createdate',
    'hs_analytics_source', 'hs_analytics_source_data_1', 'hs_analytics_source_data_2',
    'pipeline', 'deal_currency_code',
  ];

  while (true) {
    const body = {
      limit: 100,
      properties,
      filterGroups: [{
        filters: [{
          propertyName: 'createdate',
          operator: 'GTE',
          value: new Date(since).getTime(),
        }],
      }],
      sorts: [{ propertyName: 'createdate', direction: 'ASCENDING' }],
    };
    if (after) body.after = after;

    const data = await retry(() => hsPost('/crm/v3/objects/deals/search', body));
    deals.push(...data.results);

    if (!data.paging?.next?.after) break;
    after = data.paging.next.after;
    console.log(`  Fetched ${deals.length} deals so far...`);
  }

  return deals;
}

async function getContactCountry(contactId) {
  try {
    const data = await hsGet(`/crm/v3/objects/contacts/${contactId}`, {
      properties: 'country,hs_analytics_source,hs_analytics_source_data_1',
    });
    return {
      country: data.properties?.country || '',
      source: data.properties?.hs_analytics_source || '',
      sourceDetail: data.properties?.hs_analytics_source_data_1 || '',
    };
  } catch {
    return { country: '', source: '', sourceDetail: '' };
  }
}

async function enrichDealsWithContacts(deals) {
  // Batch fetch contact associations
  const dealIds = deals.map(d => d.id);
  const enriched = [];

  // Process in batches of 20
  for (let i = 0; i < deals.length; i += 20) {
    const batch = deals.slice(i, i + 20);
    const results = await Promise.all(
      batch.map(async (deal) => {
        let contactInfo = { country: '', source: '', sourceDetail: '' };

        // Try to get associated contact
        try {
          const assocData = await hsGet(`/crm/v3/objects/deals/${deal.id}/associations/contacts`);
          if (assocData.results?.length > 0) {
            contactInfo = await getContactCountry(assocData.results[0].id);
          }
        } catch {}

        const props = deal.properties;
        const source = props.hs_analytics_source || contactInfo.source || '';
        const sourceDetail = props.hs_analytics_source_data_1 || contactInfo.sourceDetail || '';

        return {
          id: deal.id,
          name: props.dealname || '',
          stage: mapStage(props.dealstage || ''),
          rawStage: props.dealstage || '',
          amount: parseFloat(props.amount || 0),
          currency: props.deal_currency_code || 'EUR',
          createDate: props.createdate ? props.createdate.slice(0, 10) : '',
          closeDate: props.closedate ? props.closedate.slice(0, 10) : '',
          week: props.createdate ? getWeekStart(props.createdate) : '',
          channel: classifyChannel(source, sourceDetail),
          geo: classifyGeo(contactInfo.country),
          source,
          sourceDetail,
        };
      })
    );
    enriched.push(...results);

    if (i + 20 < deals.length) {
      console.log(`  Enriched ${enriched.length}/${deals.length} deals...`);
    }
  }

  return enriched;
}

async function main() {
  const { start } = getDateRange(365);
  console.log(`Fetching HubSpot deals since ${start}`);

  const deals = await fetchAllDeals(start);
  console.log(`Fetched ${deals.length} deals, enriching with contact data...`);

  const enriched = await enrichDealsWithContacts(deals);

  saveRaw('hubspot-deals.json', enriched);
  console.log(`Done: ${enriched.length} deals saved`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
