/**
 * Fetch HubSpot deals for Amberscript.
 * Filters: Inbound Sales + NQL pipelines, brand_source = Amberscript.
 * Outputs: raw/hubspot-deals.json
 */

import { getDateRange, getWeekStart, getMonth, saveRaw, retry } from './utils.mjs';

const TOKEN = process.env.HUBSPOT_AMBERSCRIPT_TOKEN;
if (!TOKEN) {
  console.error('Missing HUBSPOT_AMBERSCRIPT_TOKEN');
  process.exit(1);
}

const HS_BASE = 'https://api.hubapi.com';

// Pipeline IDs
const INBOUND_PIPELINE = '4572911';
const NQL_PIPELINE = '26028994';

// Stage ID -> label mapping
const STAGES = {
  // Inbound Sales Pipeline
  '17208523': 'Prospect Requested more information',
  '17208524': 'First email / call done',
  '17208525': 'Reaction received',
  '17208526': 'Meeting',
  '17208527': 'Offer sent',
  '17208535': 'Negotiations & Follow-up',
  '17208528': 'Closed won',
  '17208529': 'Closed lost',
  // NQL Pipeline
  '80937963': 'NQL created',
  '80937964': 'First email / call done',
  '80937965': 'Reaction received',
  '80937966': 'Meeting',
  '80937967': 'Offer sent',
  '80938173': 'Negotiations & Follow-up',
  '80937968': 'Closed won',
  '80937969': 'Closed lost',
};

const PROPERTIES = [
  'dealname', 'dealstage', 'pipeline', 'amount', 'deal_currency_code',
  'createdate', 'closedate',
  'lifecycle_stage',
  'brand_source',
  'deal_product', 'deal_transcription_style', 'deal_additional_options', 'deal_form_id',
  'hubspot_owner_id',
];

async function hsGet(path) {
  const res = await fetch(`${HS_BASE}${path}`, {
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

async function fetchOwnerNames(ownerIds) {
  const map = {};
  const data = await retry(() => hsGet('/crm/v3/owners'));
  for (const owner of data.results || []) {
    if (ownerIds.has(owner.id)) {
      const first = owner.firstName || '';
      const last = owner.lastName || '';
      map[owner.id] = `${first} ${last}`.trim() || owner.email || owner.id;
    }
  }
  return map;
}

async function fetchAllDeals(since) {
  const deals = [];
  let after = undefined;

  while (true) {
    const body = {
      limit: 100,
      properties: PROPERTIES,
      filterGroups: [
        {
          filters: [
            { propertyName: 'createdate', operator: 'GTE', value: new Date(since).getTime() },
            { propertyName: 'pipeline', operator: 'EQ', value: INBOUND_PIPELINE },
            { propertyName: 'brand_source', operator: 'EQ', value: 'Amberscript' },
          ],
        },
        {
          filters: [
            { propertyName: 'createdate', operator: 'GTE', value: new Date(since).getTime() },
            { propertyName: 'pipeline', operator: 'EQ', value: NQL_PIPELINE },
            { propertyName: 'brand_source', operator: 'EQ', value: 'Amberscript' },
          ],
        },
      ],
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

function getDealStatus(stageId) {
  const label = STAGES[stageId] || '';
  if (label.startsWith('Closed won')) return 'Won';
  if (label.startsWith('Closed lost')) return 'Lost';
  return 'Open';
}

async function main() {
  const { start } = getDateRange();
  console.log(`Fetching HubSpot deals since ${start}`);

  const rawDeals = await fetchAllDeals(start);
  console.log(`Fetched ${rawDeals.length} deals`);

  // Resolve owner names
  const ownerIds = new Set(rawDeals.map(d => d.properties.hubspot_owner_id).filter(Boolean));
  const ownerNames = await fetchOwnerNames(ownerIds);
  console.log(`Resolved ${Object.keys(ownerNames).length} owner names`);

  const output = rawDeals.map(deal => {
    const p = deal.properties;
    const createDate = p.createdate ? p.createdate.slice(0, 10) : '';
    const closeDate = p.closedate ? p.closedate.slice(0, 10) : '';
    const ownerId = p.hubspot_owner_id || '';

    return {
      id: deal.id,
      name: p.dealname || '',
      pipeline: p.pipeline || '',
      stage: STAGES[p.dealstage] || p.dealstage || '',
      stageId: p.dealstage || '',
      status: getDealStatus(p.dealstage),
      lifecycleStage: p.lifecycle_stage || '',
      amount: parseFloat(p.amount || 0),
      currency: p.deal_currency_code || 'EUR',
      createDate,
      createWeek: createDate ? getWeekStart(createDate) : '',
      createMonth: createDate ? getMonth(createDate) : '',
      closeDate,
      closeWeek: closeDate ? getWeekStart(closeDate) : '',
      closeMonth: closeDate ? getMonth(closeDate) : '',
      product: p.deal_product || '',
      transcriptionStyle: p.deal_transcription_style || '',
      additionalOptions: p.deal_additional_options || '',
      formId: p.deal_form_id || '',
      ownerId,
      ownerName: ownerNames[ownerId] || ownerId,
    };
  });

  saveRaw('hubspot-deals.json', output);
  console.log(`Done: ${output.length} deals saved`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
