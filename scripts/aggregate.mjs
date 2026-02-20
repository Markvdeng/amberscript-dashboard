/**
 * Aggregate all raw data sources into data.json for the dashboard.
 * Reads from raw/ directory, outputs data.json at project root.
 *
 * Data sources:
 *   - Google Ads: cost per campaign per week (with country, product, userType)
 *   - HubSpot: deals with lifecycle stages (MQL, SQL, Customer)
 *   - GA4: form submissions (generate_lead) + purchases
 *   - Stripe: charges with plan type, product, country
 *
 * Joins:
 *   - HubSpot deal.formId → GA4 formSubmission.formId → channel attribution
 *   - Stripe charge.paymentIdentifier/uploadBatchId → GA4 purchase.transactionId → channel attribution
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { getMonth } from './utils.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');
const RAW = join(ROOT, 'raw');

function loadRaw(file) {
  const path = join(RAW, file);
  if (!existsSync(path)) {
    console.warn(`  Missing raw/${file}, using empty`);
    return null;
  }
  return JSON.parse(readFileSync(path, 'utf8'));
}

function round(n, decimals = 2) {
  return Math.round(n * Math.pow(10, decimals)) / Math.pow(10, decimals);
}

function groupBy(arr, keyFn) {
  const map = {};
  for (const item of arr) {
    const key = keyFn(item);
    if (!map[key]) map[key] = [];
    map[key].push(item);
  }
  return map;
}

// Business Unit classification
function getBU(product) {
  if (!product) return 'Transcription';
  const media = ['Human-Made Subtitles', 'Machine-Made Subtitles', 'Translated Subtitles', 'Translations', 'Subtitles'];
  const innovations = ['Amber Notes', 'AI Meeting Notes', 'Meeting Notes'];
  if (media.includes(product)) return 'Media';
  if (innovations.includes(product)) return 'Innovations';
  return 'Transcription';
}

function main() {
  console.log('Aggregating data...');

  // === LOAD RAW DATA ===
  const googleAds = loadRaw('google-ads.json') || [];
  const stripeCharges = loadRaw('stripe-charges.json') || [];
  const hubspotDeals = loadRaw('hubspot-deals.json') || [];
  const ga4Raw = loadRaw('ga4.json') || { formSubmissions: [], purchases: [] };
  const ga4 = Array.isArray(ga4Raw) ? { formSubmissions: [], purchases: [] } : ga4Raw;
  const FORM_PRODUCT_IDS = {
    '48': 'Translated Subtitles',
    '50': 'Machine-Made Transcription',
    '51': 'Human-Made Transcription',
    '52': 'Machine-Made Subtitles',
    '53': 'Human-Made Subtitles',
    '58': 'Other',
  };
  const formSubmissions = (ga4.formSubmissions || []).map(f => ({
    ...f,
    product: FORM_PRODUCT_IDS[f.product] || f.product,
  }));
  const ga4Purchases = ga4.purchases || [];

  // Stripe subscriptions (new format with snapshot + individual subs)
  const stripSubsRaw = loadRaw('stripe-subs.json');
  const stripeSubs = stripSubsRaw?.subscriptions || [];
  const subSnapshot = stripSubsRaw?.snapshot || null;

  console.log(`  Google Ads: ${googleAds.length} rows`);
  console.log(`  Stripe: ${stripeCharges.length} charges, ${stripeSubs.length} subscriptions`);
  console.log(`  HubSpot: ${hubspotDeals.length} deals`);
  console.log(`  GA4: ${formSubmissions.length} form submissions, ${ga4Purchases.length} purchases`);
  if (subSnapshot) console.log(`  MRR snapshot: ${subSnapshot.mrr} (${subSnapshot.totalActiveSubs} active subs)`);

  // === BUILD LOOKUP TABLES ===

  // GA4 formId → channel (take the most common channel per formId)
  const formChannelMap = {};
  for (const f of formSubmissions) {
    if (!f.formId) continue;
    if (!formChannelMap[f.formId]) formChannelMap[f.formId] = {};
    formChannelMap[f.formId][f.channel] = (formChannelMap[f.formId][f.channel] || 0) + f.count;
  }
  const formToChannel = {};
  for (const [formId, channels] of Object.entries(formChannelMap)) {
    formToChannel[formId] = Object.entries(channels).sort((a, b) => b[1] - a[1])[0][0];
  }

  // GA4 purchase transactionId → { channel, campaign }
  const txLookup = {};
  for (const p of ga4Purchases) {
    if (p.transactionId) {
      txLookup[p.transactionId] = { channel: p.channel, campaign: p.campaign };
    }
  }

  // Enrich HubSpot deals with channel from GA4
  for (const deal of hubspotDeals) {
    deal.channel = deal.formId ? (formToChannel[deal.formId] || 'Unknown') : 'Unknown';
  }

  // Classify Stripe charges (handle both old and new raw format)
  for (const charge of stripeCharges) {
    // If old format (has metadata object but no planType), derive fields
    if (!charge.planType && charge.metadata) {
      const meta = charge.metadata;
      const desc = charge.description || '';
      const countryKey = Object.keys(meta).find(k => /^P1_[A-Z]{2}_\d+$/.test(k));
      charge.country = charge.country || (countryKey ? countryKey.split('_')[1] : '');
      charge.product = charge.product || (meta.jobType === 'perfect' ? 'Human-Made' : 'Machine-Made');
      charge.planType = /subscription/i.test(desc) ? 'Subscription' : /invoice/i.test(desc) ? 'Invoice' : 'Prepaid';
      if (charge.planType === 'Subscription') {
        charge.planSubtype = /creation/i.test(desc) ? 'Creation' : 'Update';
      } else if (charge.planType === 'Prepaid') {
        charge.planSubtype = meta.uploadBatchId === 'addCredit' ? 'Top-Up' : 'Job Creation';
      }
      charge.paymentIdentifier = charge.paymentIdentifier || meta.paymentIdentifier || '';
      charge.uploadBatchId = charge.uploadBatchId || meta.uploadBatchId || '';
    }
  }

  // Enrich Stripe charges with channel from GA4 purchases
  let stripeMatched = 0;
  for (const charge of stripeCharges) {
    const payId = charge.paymentIdentifier || '';
    const upId = charge.uploadBatchId || '';
    let match = null;
    if (payId && txLookup[payId]) {
      match = txLookup[payId];
    } else if (upId && upId !== 'addCredit' && txLookup[upId]) {
      match = txLookup[upId];
    }
    if (match) {
      charge.channel = match.channel;
      charge.campaign = match.campaign;
      stripeMatched++;
    } else {
      charge.channel = charge.planType === 'Subscription' ? 'Subscription' : 'Unknown';
      charge.campaign = '';
    }
  }
  console.log(`  Stripe→GA4 matched: ${stripeMatched}/${stripeCharges.length}`);

  // === COLLECT ALL WEEKS ===
  const allWeeks = new Set();
  googleAds.forEach(r => allWeeks.add(r.week));
  stripeCharges.forEach(r => allWeeks.add(r.week));
  hubspotDeals.forEach(r => { if (r.createWeek) allWeeks.add(r.createWeek); });
  formSubmissions.forEach(r => { if (r.week) allWeeks.add(r.week); });
  ga4Purchases.forEach(r => { if (r.week) allWeeks.add(r.week); });
  const weeks = [...allWeeks].sort();

  // === WEEKLY: GOOGLE ADS ===
  const adsByWeek = groupBy(googleAds, r => r.week);
  const weeklyAds = weeks.map(week => {
    const rows = adsByWeek[week] || [];
    const totalCost = rows.reduce((s, r) => s + r.cost, 0);
    const mmCost = rows.filter(r => r.userType === 'Machine-Made').reduce((s, r) => s + r.cost, 0);
    const hmCost = rows.filter(r => r.userType === 'Human-Made').reduce((s, r) => s + r.cost, 0);
    const brandCost = rows.filter(r => r.campaignType === 'Brand').reduce((s, r) => s + r.cost, 0);

    const byCountry = {};
    for (const r of rows) {
      byCountry[r.country] = (byCountry[r.country] || 0) + r.cost;
    }
    const byProduct = {};
    const byBU = {};
    for (const r of rows) {
      byProduct[r.product] = (byProduct[r.product] || 0) + r.cost;
      const bu = getBU(r.product);
      if (!byBU[bu]) byBU[bu] = { totalCost: 0, mmCost: 0, hmCost: 0, brandCost: 0 };
      byBU[bu].totalCost += r.cost;
      if (r.userType === 'Machine-Made') byBU[bu].mmCost += r.cost;
      if (r.userType === 'Human-Made') byBU[bu].hmCost += r.cost;
      if (r.campaignType === 'Brand') byBU[bu].brandCost += r.cost;
    }
    for (const bu of Object.keys(byBU)) {
      byBU[bu] = { totalCost: round(byBU[bu].totalCost), mmCost: round(byBU[bu].mmCost), hmCost: round(byBU[bu].hmCost), brandCost: round(byBU[bu].brandCost) };
    }

    return {
      week,
      totalCost: round(totalCost),
      mmCost: round(mmCost),
      hmCost: round(hmCost),
      brandCost: round(brandCost),
      byCountry: Object.fromEntries(Object.entries(byCountry).map(([k, v]) => [k, round(v)])),
      byProduct: Object.fromEntries(Object.entries(byProduct).map(([k, v]) => [k, round(v)])),
      byBU,
    };
  });

  // === WEEKLY: FUNNEL ===
  // Leads = GA4 generate_lead events (form submissions)
  // MQLs = all HubSpot deals (all are at least MQL in these pipelines)
  // SQLs = deals with lifecycleStage in [SQL, Customer]
  // Won = deals with status === 'Won'
  // "from Leads" = deals that have a formId (traceable to GA4 form submission)
  const dealsByWeek = groupBy(hubspotDeals, r => r.createWeek);
  const formsByWeek = groupBy(formSubmissions, r => r.week);
  const weeklyFunnel = weeks.map(week => {
    const deals = dealsByWeek[week] || [];
    const forms = formsByWeek[week] || [];
    const leads = forms.reduce((s, r) => s + r.count, 0);
    const mqls = deals.length;
    const mqlsFromLeads = deals.filter(r => r.formId).length;
    const sqls = deals.filter(r => ['SQL', 'Customer'].includes(r.lifecycleStage)).length;
    const sqlsFromLeads = deals.filter(r => r.formId && ['SQL', 'Customer'].includes(r.lifecycleStage)).length;
    const won = deals.filter(r => r.status === 'Won').length;
    const wonRevenue = deals.filter(r => r.status === 'Won').reduce((s, r) => s + (r.amount || 0), 0);

    const byChannel = {};
    const byCountry = {};
    for (const r of deals) {
      const ch = r.channel || 'Unknown';
      if (!byChannel[ch]) byChannel[ch] = { mqls: 0, sqls: 0, won: 0, wonRevenue: 0 };
      byChannel[ch].mqls++;
      if (['SQL', 'Customer'].includes(r.lifecycleStage)) byChannel[ch].sqls++;
      if (r.status === 'Won') { byChannel[ch].won++; byChannel[ch].wonRevenue += r.amount || 0; }

      // Country from form name (e.g. RequestQuote-nl → NL)
      const lang = r.formId ? r.formId.match(/-([a-z]{2})_/)?.[1]?.toUpperCase() : '';
      const country = lang || 'Unknown';
      if (!byCountry[country]) byCountry[country] = { mqls: 0, sqls: 0, won: 0, wonRevenue: 0 };
      byCountry[country].mqls++;
      if (['SQL', 'Customer'].includes(r.lifecycleStage)) byCountry[country].sqls++;
      if (r.status === 'Won') { byCountry[country].won++; byCountry[country].wonRevenue += r.amount || 0; }
    }

    return {
      week, leads, mqls, mqlsFromLeads, sqls, sqlsFromLeads, won, wonRevenue: round(wonRevenue),
      byChannel: roundObj(byChannel),
      byCountry: roundObj(byCountry),
    };
  });

  // === WEEKLY: STRIPE ===
  const chargesByWeek = groupBy(stripeCharges, r => r.week);
  const weeklyStripe = weeks.map(week => {
    const rows = chargesByWeek[week] || [];
    const totalRevenue = rows.reduce((s, r) => s + r.amount, 0);

    const byPlanType = {};
    const byProduct = {};
    const byCountry = {};
    const byCurrency = {};
    const byChannel = {};

    for (const r of rows) {
      addTo(byPlanType, r.planType, r);
      addTo(byProduct, r.product, r);
      addTo(byCountry, r.country || 'Unknown', r);
      addTo(byCurrency, r.currency, r);
      addTo(byChannel, r.channel, r);
    }

    // Pre-computed filter segments for dashboard cascading dropdowns
    const seg = computeSegments(rows);

    return {
      week, totalRevenue: round(totalRevenue), count: rows.length,
      seg,
      byPlanType: roundCountRev(byPlanType),
      byProduct: roundCountRev(byProduct),
      byCountry: roundCountRev(byCountry),
      byCurrency: roundCountRev(byCurrency),
      byChannel: roundCountRev(byChannel),
    };
  });

  // === WEEKLY: GA4 PURCHASES ===
  const purchasesByWeek = groupBy(ga4Purchases, r => r.week);
  const weeklyGA4 = weeks.map(week => {
    const purchases = purchasesByWeek[week] || [];
    return {
      week,
      purchases: purchases.reduce((s, r) => s + r.transactions, 0),
      purchaseRevenue: round(purchases.reduce((s, r) => s + r.revenue, 0)),
    };
  });

  // === MONTHLY AGGREGATES ===
  const allMonths = [...new Set(weeks.map(w => getMonth(w)))].sort();
  const monthlyData = allMonths.map(month => {
    const monthWeeks = weeks.filter(w => getMonth(w) === month);

    // Ads
    const adRows = monthWeeks.flatMap(w => adsByWeek[w] || []);
    const adsCost = round(adRows.reduce((s, r) => s + r.cost, 0));
    const mmCost = round(adRows.filter(r => r.userType === 'Machine-Made').reduce((s, r) => s + r.cost, 0));
    const hmCost = round(adRows.filter(r => r.userType === 'Human-Made').reduce((s, r) => s + r.cost, 0));
    const brandCost = round(adRows.filter(r => r.campaignType === 'Brand').reduce((s, r) => s + r.cost, 0));

    // Funnel: Leads (GA4) → MQLs (HubSpot) → SQLs → Won
    const dealRows = monthWeeks.flatMap(w => dealsByWeek[w] || []);
    const formRows = monthWeeks.flatMap(w => formsByWeek[w] || []);
    const leads = formRows.reduce((s, r) => s + r.count, 0);
    const mqls = dealRows.length;
    const mqlsFromLeads = dealRows.filter(r => r.formId).length;
    const sqls = dealRows.filter(r => ['SQL', 'Customer'].includes(r.lifecycleStage)).length;
    const sqlsFromLeads = dealRows.filter(r => r.formId && ['SQL', 'Customer'].includes(r.lifecycleStage)).length;
    const won = dealRows.filter(r => r.status === 'Won').length;
    const wonRevenue = round(dealRows.filter(r => r.status === 'Won').reduce((s, r) => s + (r.amount || 0), 0));

    // Conversion rates
    const mqlToSql = mqls > 0 ? round(sqls / mqls * 100, 1) : 0;
    const sqlToWon = sqls > 0 ? round(won / sqls * 100, 1) : 0;

    // Stripe
    const chargeRows = monthWeeks.flatMap(w => chargesByWeek[w] || []);
    const stripeRevenue = round(chargeRows.reduce((s, r) => s + r.amount, 0));
    const stripeSeg = computeSegments(chargeRows);

    return {
      month, adsCost, mmCost, hmCost, brandCost,
      leads, mqls, mqlsFromLeads, sqls, sqlsFromLeads, won, wonRevenue,
      mqlToSql, sqlToWon,
      stripeRevenue, stripeSeg,
    };
  });

  // === KPIs ===
  const totalLeads = formSubmissions.reduce((s, r) => s + r.count, 0);
  const totalMQLs = hubspotDeals.length;
  const totalMQLsFromLeads = hubspotDeals.filter(r => r.formId).length;
  const totalSQLs = hubspotDeals.filter(r => ['SQL', 'Customer'].includes(r.lifecycleStage)).length;
  const totalSQLsFromLeads = hubspotDeals.filter(r => r.formId && ['SQL', 'Customer'].includes(r.lifecycleStage)).length;
  const totalWon = hubspotDeals.filter(r => r.status === 'Won').length;
  const totalDealRevenue = round(hubspotDeals.filter(r => r.status === 'Won').reduce((s, r) => s + (r.amount || 0), 0));
  const totalStripeRevenue = round(stripeCharges.reduce((s, r) => s + r.amount, 0));
  const totalAdsCost = round(googleAds.reduce((s, r) => s + r.cost, 0));
  const roas = totalAdsCost > 0 ? round(totalStripeRevenue / totalAdsCost, 2) : 0;

  // === DEAL-LEVEL DATA (for client-side filtering) ===
  const deals = hubspotDeals.map(d => ({
    week: d.createWeek,
    closeWeek: d.closeWeek || '',
    product: d.product || '',
    bu: getBU(d.product),
    transcriptionStyle: d.transcriptionStyle || '',
    additionalOptions: d.additionalOptions || '',
    country: d.country || '',
    channel: d.channel || 'Unknown',
    ownerName: d.ownerName || d.ownerId || '',
    lifecycleStage: d.lifecycleStage || '',
    status: d.status || '',
    amount: d.amount || 0,
    formId: d.formId || '',
  }));

  // GA4 forms by week + country (for lead filtering by country)
  const ga4Forms = formSubmissions.map(f => ({
    week: f.week,
    country: f.country || '',
    channel: f.channel || '',
    product: f.product || '',
    bu: getBU(f.product),
    count: f.count,
  }));

  // === SUBSCRIPTION METRICS (weekly new, churn, MRR) ===
  const subsByCreatedWeek = groupBy(stripeSubs.filter(s => s.createdWeek), s => s.createdWeek);
  const subsByCanceledWeek = groupBy(stripeSubs.filter(s => s.canceledWeek), s => s.canceledWeek);
  const weeklySubMetrics = weeks.map(week => {
    const created = subsByCreatedWeek[week] || [];
    const canceled = subsByCanceledWeek[week] || [];
    const newMrr = round(created.reduce((s, r) => s + (r.monthlyAmount || 0), 0));
    const churnedMrr = round(canceled.reduce((s, r) => s + (r.monthlyAmount || 0), 0));
    return {
      week,
      newSubs: created.length,
      newMrr,
      churnedSubs: canceled.length,
      churnedMrr,
      netMrr: round(newMrr - churnedMrr),
      byPlan: {
        monthly: { new: created.filter(s => s.planType === 'monthly').length, churned: canceled.filter(s => s.planType === 'monthly').length },
        yearly: { new: created.filter(s => s.planType === 'yearly').length, churned: canceled.filter(s => s.planType === 'yearly').length },
      },
    };
  });

  // === BUILD OUTPUT ===
  const output = {
    updatedAt: new Date().toISOString(),
    dateRange: { start: weeks[0] || '', end: weeks[weeks.length - 1] || '' },
    kpis: {
      totalLeads, totalMQLs, totalMQLsFromLeads, totalSQLs, totalSQLsFromLeads, totalWon,
      totalDealRevenue, totalStripeRevenue, totalAdsCost, roas,
    },
    subSnapshot,
    monthly: monthlyData,
    weekly: {
      ads: weeklyAds,
      funnel: weeklyFunnel,
      stripe: weeklyStripe,
      ga4: weeklyGA4,
      subs: weeklySubMetrics,
    },
    deals,
    ga4Forms,
  };

  writeFileSync(join(ROOT, 'data.json'), JSON.stringify(output, null, 2));
  console.log(`data.json written: ${allMonths.length} months, ${weeks.length} weeks`);
}

// Helper: compute count + rounded revenue for a filtered set
function cr(rows) {
  return {
    count: rows.length,
    revenue: round(rows.reduce((s, r) => s + r.amount, 0)),
  };
}

// Pre-compute all filter segment combinations for cascading dropdowns
// Dropdown 1 (product): all | machine-made | human-made | invoice
// Dropdown 2 (plan type, when product=all or machine-made): all | prepaid | subscription
// Dropdown 3 (subtype): all | topup/job (prepaid) or update/creation (subscription)
function computeSegments(rows) {
  const mm = rows.filter(r => r.product === 'Machine-Made' && r.planType !== 'Invoice');
  return {
    // Product = All
    'all.all.all':        cr(rows),
    'all.prepaid.all':    cr(rows.filter(r => r.planType === 'Prepaid')),
    'all.prepaid.topup':  cr(rows.filter(r => r.planType === 'Prepaid' && r.planSubtype === 'Top-Up')),
    'all.prepaid.job':    cr(rows.filter(r => r.planType === 'Prepaid' && r.planSubtype === 'Job Creation')),
    'all.sub.all':        cr(rows.filter(r => r.planType === 'Subscription')),
    'all.sub.update':     cr(rows.filter(r => r.planType === 'Subscription' && r.planSubtype === 'Update')),
    'all.sub.creation':   cr(rows.filter(r => r.planType === 'Subscription' && r.planSubtype === 'Creation')),
    // Product = Machine-Made (excludes invoice)
    'mm.all.all':         cr(mm),
    'mm.prepaid.all':     cr(mm.filter(r => r.planType === 'Prepaid')),
    'mm.prepaid.topup':   cr(mm.filter(r => r.planType === 'Prepaid' && r.planSubtype === 'Top-Up')),
    'mm.prepaid.job':     cr(mm.filter(r => r.planType === 'Prepaid' && r.planSubtype === 'Job Creation')),
    'mm.sub.all':         cr(mm.filter(r => r.planType === 'Subscription')),
    'mm.sub.update':      cr(mm.filter(r => r.planType === 'Subscription' && r.planSubtype === 'Update')),
    'mm.sub.creation':    cr(mm.filter(r => r.planType === 'Subscription' && r.planSubtype === 'Creation')),
    // Product = Human-Made
    'hm.all.all':         cr(rows.filter(r => r.product === 'Human-Made')),
    // Product = Invoice
    'inv.all.all':        cr(rows.filter(r => r.planType === 'Invoice')),
  };
}

// Helper: add count/revenue to a bucket
function addTo(obj, key, charge) {
  if (!obj[key]) obj[key] = { count: 0, revenue: 0 };
  obj[key].count++;
  obj[key].revenue += charge.amount;
}

// Helper: round revenue in { count, revenue } objects
function roundCountRev(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    out[k] = { count: v.count, revenue: round(v.revenue) };
  }
  return out;
}

// Helper: round wonRevenue in funnel breakdown objects
function roundObj(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    out[k] = { ...v, wonRevenue: round(v.wonRevenue || 0) };
  }
  return out;
}

main();
