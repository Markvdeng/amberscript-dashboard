/**
 * Aggregate all raw data sources into data.json for the dashboard.
 * Reads from raw/ directory, outputs data.json at project root.
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { getMonth, getWeekStart } from './utils.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');
const RAW = join(ROOT, 'raw');

function loadRaw(file) {
  const path = join(RAW, file);
  if (!existsSync(path)) {
    console.warn(`Missing raw/${file}, using empty array`);
    return [];
  }
  return JSON.parse(readFileSync(path, 'utf8'));
}

function sumBy(arr, keyFn, valueFn) {
  const map = {};
  for (const item of arr) {
    const key = keyFn(item);
    if (!map[key]) map[key] = 0;
    map[key] += valueFn(item);
  }
  return map;
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

function round(n, decimals = 2) {
  return Math.round(n * Math.pow(10, decimals)) / Math.pow(10, decimals);
}

function main() {
  console.log('Aggregating data...');

  // Load raw data
  const googleAds = loadRaw('google-ads.json');
  const stripeCharges = loadRaw('stripe-charges.json');
  const stripeSubs = loadRaw('stripe-subs.json');
  const hubspotDeals = loadRaw('hubspot-deals.json');
  const ga4Raw = loadRaw('ga4.json');
  const ga4 = Array.isArray(ga4Raw) ? { formSubmissions: [], purchases: [] } : ga4Raw;

  // Collect all weeks
  const allWeeks = new Set();
  googleAds.forEach(r => allWeeks.add(r.week));
  stripeCharges.forEach(r => allWeeks.add(r.week));
  stripeSubs.forEach(r => allWeeks.add(r.week));
  hubspotDeals.forEach(r => { if (r.week) allWeeks.add(r.week); });
  (ga4.formSubmissions || []).forEach(r => { if (r.week) allWeeks.add(r.week); });
  (ga4.purchases || []).forEach(r => { if (r.week) allWeeks.add(r.week); });

  const weeks = [...allWeeks].sort();

  // === WEEKLY DATA ===

  // Google Ads: weekly cost by productType
  const adsByWeek = groupBy(googleAds, r => r.week);
  const weeklyAds = weeks.map(week => {
    const rows = adsByWeek[week] || [];
    const totalCost = rows.reduce((s, r) => s + r.cost, 0);
    const mmCost = rows.filter(r => r.productType === 'Machine-Made').reduce((s, r) => s + r.cost, 0);
    const hmCost = rows.filter(r => r.productType === 'Human-Made').reduce((s, r) => s + r.cost, 0);
    const notesCost = rows.filter(r => r.productType === 'AmberNotes').reduce((s, r) => s + r.cost, 0);
    const totalClicks = rows.reduce((s, r) => s + r.clicks, 0);
    const totalConversions = rows.reduce((s, r) => s + r.conversions, 0);
    return { week, totalCost: round(totalCost), mmCost: round(mmCost), hmCost: round(hmCost), notesCost: round(notesCost), clicks: totalClicks, conversions: round(totalConversions) };
  });

  // HubSpot: weekly funnel by channel and geo
  const dealsByWeek = groupBy(hubspotDeals, r => r.week);
  const weeklyFunnel = weeks.map(week => {
    const rows = dealsByWeek[week] || [];
    const leads = rows.filter(r => ['lead', 'MQL', 'SQL', 'closed-won', 'closed-lost'].includes(r.stage)).length;
    const mqls = rows.filter(r => ['MQL', 'SQL', 'closed-won', 'closed-lost'].includes(r.stage)).length;
    const sqls = rows.filter(r => ['SQL', 'closed-won', 'closed-lost'].includes(r.stage)).length;
    const closedWon = rows.filter(r => r.stage === 'closed-won').length;
    const closedRevenue = rows.filter(r => r.stage === 'closed-won').reduce((s, r) => s + r.amount, 0);

    // By channel
    const channels = {};
    for (const r of rows) {
      if (!channels[r.channel]) channels[r.channel] = { leads: 0, mqls: 0, sqls: 0, deals: 0, revenue: 0 };
      channels[r.channel].leads++;
      if (['MQL', 'SQL', 'closed-won', 'closed-lost'].includes(r.stage)) channels[r.channel].mqls++;
      if (['SQL', 'closed-won', 'closed-lost'].includes(r.stage)) channels[r.channel].sqls++;
      if (r.stage === 'closed-won') {
        channels[r.channel].deals++;
        channels[r.channel].revenue += r.amount;
      }
    }

    // By geo
    const geos = {};
    for (const r of rows) {
      if (!geos[r.geo]) geos[r.geo] = { leads: 0, mqls: 0, sqls: 0, deals: 0, revenue: 0 };
      geos[r.geo].leads++;
      if (['MQL', 'SQL', 'closed-won', 'closed-lost'].includes(r.stage)) geos[r.geo].mqls++;
      if (['SQL', 'closed-won', 'closed-lost'].includes(r.stage)) geos[r.geo].sqls++;
      if (r.stage === 'closed-won') {
        geos[r.geo].deals++;
        geos[r.geo].revenue += r.amount;
      }
    }

    return {
      week, leads, mqls, sqls, closedWon, closedRevenue: round(closedRevenue),
      byChannel: channels, byGeo: geos,
    };
  });

  // Stripe charges: weekly by type
  const chargesByWeek = groupBy(stripeCharges, r => r.week);
  const weeklyStripe = weeks.map(week => {
    const rows = chargesByWeek[week] || [];
    const totalRevenue = rows.reduce((s, r) => s + r.amount, 0);
    const byType = {};
    for (const r of rows) {
      if (!byType[r.type]) byType[r.type] = { count: 0, revenue: 0 };
      byType[r.type].count++;
      byType[r.type].revenue += r.amount;
    }
    const byCurrency = {};
    for (const r of rows) {
      if (!byCurrency[r.currency]) byCurrency[r.currency] = { count: 0, revenue: 0 };
      byCurrency[r.currency].count++;
      byCurrency[r.currency].revenue += r.amount;
    }
    return { week, totalRevenue: round(totalRevenue), count: rows.length, byType, byCurrency };
  });

  // Stripe subscriptions: weekly by plan
  const subsByWeek = groupBy(stripeSubs, r => r.week);
  const weeklySubs = weeks.map(week => {
    const rows = subsByWeek[week] || [];
    const monthly = rows.filter(r => r.planType === 'monthly').length;
    const yearly = rows.filter(r => r.planType === 'yearly').length;
    return { week, total: rows.length, monthly, yearly };
  });

  // GA4: weekly form submissions and purchases
  const formsByWeek = groupBy(ga4.formSubmissions || [], r => r.week);
  const purchasesByWeek = groupBy(ga4.purchases || [], r => r.week);
  const weeklyGA4 = weeks.map(week => {
    const forms = formsByWeek[week] || [];
    const purchases = purchasesByWeek[week] || [];
    return {
      week,
      formSubmissions: forms.reduce((s, r) => s + r.count, 0),
      purchases: purchases.reduce((s, r) => s + r.count, 0),
      purchaseValue: round(purchases.reduce((s, r) => s + r.value, 0)),
    };
  });

  // === MONTHLY AGGREGATES ===
  const allMonths = [...new Set(weeks.map(w => getMonth(w)))].sort();
  const monthlyData = allMonths.map(month => {
    const monthWeeks = weeks.filter(w => getMonth(w) === month);

    // Ads
    const adRows = monthWeeks.flatMap(w => adsByWeek[w] || []);
    const adsCost = round(adRows.reduce((s, r) => s + r.cost, 0));
    const mmCost = round(adRows.filter(r => r.productType === 'Machine-Made').reduce((s, r) => s + r.cost, 0));
    const hmCost = round(adRows.filter(r => r.productType === 'Human-Made').reduce((s, r) => s + r.cost, 0));

    // Funnel
    const dealRows = monthWeeks.flatMap(w => dealsByWeek[w] || []);
    const leads = dealRows.filter(r => ['lead', 'MQL', 'SQL', 'closed-won', 'closed-lost'].includes(r.stage)).length;
    const mqls = dealRows.filter(r => ['MQL', 'SQL', 'closed-won', 'closed-lost'].includes(r.stage)).length;
    const sqls = dealRows.filter(r => ['SQL', 'closed-won', 'closed-lost'].includes(r.stage)).length;
    const closedWon = dealRows.filter(r => r.stage === 'closed-won').length;
    const closedRevenue = round(dealRows.filter(r => r.stage === 'closed-won').reduce((s, r) => s + r.amount, 0));

    // Conversion rates
    const leadToMql = leads > 0 ? round(mqls / leads * 100, 1) : 0;
    const mqlToSql = mqls > 0 ? round(sqls / mqls * 100, 1) : 0;
    const mqlToDeal = mqls > 0 ? round(closedWon / mqls * 100, 1) : 0;
    const sqlToDeal = sqls > 0 ? round(closedWon / sqls * 100, 1) : 0;

    // Stripe
    const chargeRows = monthWeeks.flatMap(w => chargesByWeek[w] || []);
    const stripeRevenue = round(chargeRows.reduce((s, r) => s + r.amount, 0));

    // Deal AOV
    const dealAOV = closedWon > 0 ? round(closedRevenue / closedWon) : 0;

    return {
      month, adsCost, mmCost, hmCost,
      leads, mqls, sqls, closedWon, closedRevenue,
      leadToMql, mqlToSql, mqlToDeal, sqlToDeal,
      stripeRevenue, dealAOV,
    };
  });

  // === KPIs (totals across all time) ===
  const totalLeads = hubspotDeals.filter(r => ['lead', 'MQL', 'SQL', 'closed-won', 'closed-lost'].includes(r.stage)).length;
  const totalMQLs = hubspotDeals.filter(r => ['MQL', 'SQL', 'closed-won', 'closed-lost'].includes(r.stage)).length;
  const totalSQLs = hubspotDeals.filter(r => ['SQL', 'closed-won', 'closed-lost'].includes(r.stage)).length;
  const totalDeals = hubspotDeals.filter(r => r.stage === 'closed-won').length;
  const totalDealRevenue = round(hubspotDeals.filter(r => r.stage === 'closed-won').reduce((s, r) => s + r.amount, 0));
  const totalStripeRevenue = round(stripeCharges.reduce((s, r) => s + r.amount, 0));
  const totalAdsCost = round(googleAds.reduce((s, r) => s + r.cost, 0));
  const roas = totalAdsCost > 0 ? round(totalStripeRevenue / totalAdsCost, 2) : 0;

  // === BUILD OUTPUT ===
  const output = {
    updatedAt: new Date().toISOString(),
    dateRange: { start: weeks[0] || '', end: weeks[weeks.length - 1] || '' },
    kpis: {
      totalLeads, totalMQLs, totalSQLs, totalDeals,
      totalDealRevenue, totalStripeRevenue, totalAdsCost, roas,
    },
    monthly: monthlyData,
    weekly: {
      ads: weeklyAds,
      funnel: weeklyFunnel,
      stripe: weeklyStripe,
      subscriptions: weeklySubs,
      ga4: weeklyGA4,
    },
  };

  writeFileSync(join(ROOT, 'data.json'), JSON.stringify(output, null, 2));
  console.log(`data.json written: ${allMonths.length} months, ${weeks.length} weeks`);
}

main();
