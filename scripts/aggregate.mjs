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

function inc(obj, key, field, val = 1) {
  if (!obj[key]) obj[key] = { count: 0, revenue: 0, leads: 0, sqls: 0, won: 0, wonRevenue: 0 };
  obj[key][field] += val;
}

function main() {
  console.log('Aggregating data...');

  // === LOAD RAW DATA ===
  const googleAds = loadRaw('google-ads.json') || [];
  const stripeCharges = loadRaw('stripe-charges.json') || [];
  const hubspotDeals = loadRaw('hubspot-deals.json') || [];
  const ga4Raw = loadRaw('ga4.json') || { formSubmissions: [], purchases: [] };
  const ga4 = Array.isArray(ga4Raw) ? { formSubmissions: [], purchases: [] } : ga4Raw;
  const formSubmissions = ga4.formSubmissions || [];
  const ga4Purchases = ga4.purchases || [];

  console.log(`  Google Ads: ${googleAds.length} rows`);
  console.log(`  Stripe: ${stripeCharges.length} charges`);
  console.log(`  HubSpot: ${hubspotDeals.length} deals`);
  console.log(`  GA4: ${formSubmissions.length} form submissions, ${ga4Purchases.length} purchases`);

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
    for (const r of rows) {
      byProduct[r.product] = (byProduct[r.product] || 0) + r.cost;
    }

    return {
      week,
      totalCost: round(totalCost),
      mmCost: round(mmCost),
      hmCost: round(hmCost),
      brandCost: round(brandCost),
      byCountry: Object.fromEntries(Object.entries(byCountry).map(([k, v]) => [k, round(v)])),
      byProduct: Object.fromEntries(Object.entries(byProduct).map(([k, v]) => [k, round(v)])),
    };
  });

  // === WEEKLY: HUBSPOT FUNNEL ===
  // Lifecycle: MQL → SQL → Customer. All deals are at least leads.
  // SQLs = lifecycleStage in [SQL, Customer]
  // Won = status === 'Won'
  const dealsByWeek = groupBy(hubspotDeals, r => r.createWeek);
  const weeklyFunnel = weeks.map(week => {
    const rows = dealsByWeek[week] || [];
    const leads = rows.length;
    const sqls = rows.filter(r => ['SQL', 'Customer'].includes(r.lifecycleStage)).length;
    const won = rows.filter(r => r.status === 'Won').length;
    const wonRevenue = rows.filter(r => r.status === 'Won').reduce((s, r) => s + (r.amount || 0), 0);

    const byChannel = {};
    const byCountry = {};
    for (const r of rows) {
      const ch = r.channel || 'Unknown';
      if (!byChannel[ch]) byChannel[ch] = { leads: 0, sqls: 0, won: 0, wonRevenue: 0 };
      byChannel[ch].leads++;
      if (['SQL', 'Customer'].includes(r.lifecycleStage)) byChannel[ch].sqls++;
      if (r.status === 'Won') { byChannel[ch].won++; byChannel[ch].wonRevenue += r.amount || 0; }

      // Country from form name (e.g. RequestQuote-nl → NL)
      const lang = r.formId ? r.formId.match(/-([a-z]{2})_/)?.[1]?.toUpperCase() : '';
      const country = lang || 'Unknown';
      if (!byCountry[country]) byCountry[country] = { leads: 0, sqls: 0, won: 0, wonRevenue: 0 };
      byCountry[country].leads++;
      if (['SQL', 'Customer'].includes(r.lifecycleStage)) byCountry[country].sqls++;
      if (r.status === 'Won') { byCountry[country].won++; byCountry[country].wonRevenue += r.amount || 0; }
    }

    return {
      week, leads, sqls, won, wonRevenue: round(wonRevenue),
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
      if (r.country) addTo(byCountry, r.country, r);
      addTo(byCurrency, r.currency, r);
      addTo(byChannel, r.channel, r);
    }

    return {
      week, totalRevenue: round(totalRevenue), count: rows.length,
      byPlanType: roundCountRev(byPlanType),
      byProduct: roundCountRev(byProduct),
      byCountry: roundCountRev(byCountry),
      byCurrency: roundCountRev(byCurrency),
      byChannel: roundCountRev(byChannel),
    };
  });

  // === WEEKLY: GA4 ===
  const formsByWeek = groupBy(formSubmissions, r => r.week);
  const purchasesByWeek = groupBy(ga4Purchases, r => r.week);
  const weeklyGA4 = weeks.map(week => {
    const forms = formsByWeek[week] || [];
    const purchases = purchasesByWeek[week] || [];
    return {
      week,
      formSubmissions: forms.reduce((s, r) => s + r.count, 0),
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

    // Funnel
    const dealRows = monthWeeks.flatMap(w => dealsByWeek[w] || []);
    const leads = dealRows.length;
    const sqls = dealRows.filter(r => ['SQL', 'Customer'].includes(r.lifecycleStage)).length;
    const won = dealRows.filter(r => r.status === 'Won').length;
    const wonRevenue = round(dealRows.filter(r => r.status === 'Won').reduce((s, r) => s + (r.amount || 0), 0));

    // Conversion rates
    const leadToSql = leads > 0 ? round(sqls / leads * 100, 1) : 0;
    const sqlToWon = sqls > 0 ? round(won / sqls * 100, 1) : 0;

    // Stripe
    const chargeRows = monthWeeks.flatMap(w => chargesByWeek[w] || []);
    const stripeRevenue = round(chargeRows.reduce((s, r) => s + r.amount, 0));

    // GA4
    const formRows = monthWeeks.flatMap(w => formsByWeek[w] || []);
    const formSubs = formRows.reduce((s, r) => s + r.count, 0);

    return {
      month, adsCost, mmCost, hmCost, brandCost,
      leads, sqls, won, wonRevenue,
      leadToSql, sqlToWon,
      stripeRevenue, formSubmissions: formSubs,
    };
  });

  // === KPIs ===
  const totalLeads = hubspotDeals.length;
  const totalSQLs = hubspotDeals.filter(r => ['SQL', 'Customer'].includes(r.lifecycleStage)).length;
  const totalWon = hubspotDeals.filter(r => r.status === 'Won').length;
  const totalDealRevenue = round(hubspotDeals.filter(r => r.status === 'Won').reduce((s, r) => s + (r.amount || 0), 0));
  const totalStripeRevenue = round(stripeCharges.reduce((s, r) => s + r.amount, 0));
  const totalAdsCost = round(googleAds.reduce((s, r) => s + r.cost, 0));
  const roas = totalAdsCost > 0 ? round(totalStripeRevenue / totalAdsCost, 2) : 0;
  const totalFormSubmissions = formSubmissions.reduce((s, r) => s + r.count, 0);

  // === BUILD OUTPUT ===
  const output = {
    updatedAt: new Date().toISOString(),
    dateRange: { start: weeks[0] || '', end: weeks[weeks.length - 1] || '' },
    kpis: {
      totalLeads, totalSQLs, totalWon,
      totalDealRevenue, totalStripeRevenue, totalAdsCost, roas,
      totalFormSubmissions,
    },
    monthly: monthlyData,
    weekly: {
      ads: weeklyAds,
      funnel: weeklyFunnel,
      stripe: weeklyStripe,
      ga4: weeklyGA4,
    },
  };

  writeFileSync(join(ROOT, 'data.json'), JSON.stringify(output, null, 2));
  console.log(`data.json written: ${allMonths.length} months, ${weeks.length} weeks`);
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
