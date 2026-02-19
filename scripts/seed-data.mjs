/**
 * Seed data generator for the Amberscript marketing dashboard.
 * Generates 52 weeks of realistic sample data with trends and seasonal variation.
 */

import { writeFileSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_PATH = resolve(__dirname, "..", "data.json");

function mulberry32(seed) {
  return function () {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const rand = mulberry32(42);
function rf(min, max) { return min + rand() * (max - min); }
function ri(min, max) { return Math.round(rf(min, max)); }
function round(v, n = 2) { const f = 10 ** n; return Math.round(v * f) / f; }

function weekStart(index) {
  const d = new Date("2025-02-17");
  d.setDate(d.getDate() + index * 7);
  return d.toISOString().slice(0, 10);
}

function toMonth(dateStr) { return dateStr.slice(0, 7); }

function seasonalMultiplier(weekIdx) {
  const trend = 1 + (weekIdx / 52) * 0.15;
  const seasonal = 1
    + 0.08 * Math.cos(((weekIdx - 4) / 52) * 2 * Math.PI)
    + (weekIdx >= 20 && weekIdx <= 30 ? -0.06 : 0)
    + (weekIdx >= 44 && weekIdx <= 46 ? -0.10 : 0);
  return trend * seasonal;
}

function distribute(total, shares, integerMode = false) {
  const keys = Object.keys(shares);
  const raw = {}; let sum = 0;
  for (const k of keys) {
    const noise = 1 + rf(-0.15, 0.15);
    raw[k] = shares[k] * noise; sum += raw[k];
  }
  const result = {}; let allocated = 0;
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    if (i === keys.length - 1) {
      result[k] = integerMode ? Math.max(0, Math.round(total - allocated)) : round(total - allocated);
    } else {
      const v = (raw[k] / sum) * total;
      result[k] = integerMode ? Math.max(0, Math.round(v)) : round(v);
      allocated += result[k];
    }
  }
  return result;
}

const WEEKS = 52;
const weeklyAds = [];
const weeklyFunnel = [];
const weeklyStripe = [];
const weeklySubscriptions = [];
const weeklyGa4 = [];

const channelShares = {
  "Paid Search Generic": 0.4,
  Direct: 0.25,
  Organic: 0.2,
  "Paid Search Brand": 0.1,
  Other: 0.05,
};

const geoShares = { NL: 0.5, DE: 0.3, CH: 0.1, AT: 0.05, Other: 0.05 };

for (let w = 0; w < WEEKS; w++) {
  const week = weekStart(w);
  const sm = seasonalMultiplier(w);

  // Ads
  const totalCost = round(rf(3000, 4500) * sm);
  const mmShare = rf(0.57, 0.63);
  const hmShare = rf(0.27, 0.33);
  const mmCost = round(totalCost * mmShare);
  const hmCost = round(totalCost * hmShare);
  const notesCost = round(totalCost - mmCost - hmCost);
  const cpc = rf(1.8, 3.2);
  const clicks = ri((totalCost / cpc) * 0.9, (totalCost / cpc) * 1.1);
  const convRate = rf(0.03, 0.06);
  const conversions = Math.max(1, ri(clicks * convRate * 0.9, clicks * convRate * 1.1));
  weeklyAds.push({ week, totalCost, mmCost, hmCost, notesCost, clicks, conversions });

  // Funnel
  const leadsRaw = ri(Math.round(15 * sm), Math.round(30 * sm));
  const leads = Math.max(10, leadsRaw);
  const mqlRate = rf(0.4, 0.5);
  const mqls = Math.max(1, ri(leads * mqlRate * 0.95, leads * mqlRate * 1.05));
  const sqlRate = rf(0.3, 0.4);
  const sqls = Math.max(1, ri(mqls * sqlRate * 0.9, mqls * sqlRate * 1.1));
  const closeRate = rf(0.2, 0.3);
  const closedWon = Math.max(0, ri(sqls * closeRate * 0.9, sqls * closeRate * 1.1));
  const avgDeal = rf(2000, 5000);
  const closedRevenue = round(closedWon * avgDeal);

  const chLeads = distribute(leads, channelShares, true);
  const byChannel = {};
  for (const ch of Object.keys(channelShares)) {
    const cl = chLeads[ch];
    const cm = Math.max(0, ri(cl * mqlRate * 0.9, cl * mqlRate * 1.1));
    const cs = Math.max(0, ri(cm * sqlRate * 0.85, cm * sqlRate * 1.15));
    const cd = Math.max(0, ri(cs * closeRate * 0.85, cs * closeRate * 1.15));
    const cr = round(cd * rf(2000, 5000));
    byChannel[ch] = { leads: cl, mqls: cm, sqls: cs, deals: cd, revenue: cr };
  }

  const gLeads = distribute(leads, geoShares, true);
  const byGeo = {};
  for (const geo of Object.keys(geoShares)) {
    const gl = gLeads[geo];
    const gm = Math.max(0, ri(gl * mqlRate * 0.9, gl * mqlRate * 1.1));
    const gs = Math.max(0, ri(gm * sqlRate * 0.85, gm * sqlRate * 1.15));
    const gd = Math.max(0, ri(gs * closeRate * 0.85, gs * closeRate * 1.15));
    const gr = round(gd * rf(2000, 5000));
    byGeo[geo] = { leads: gl, mqls: gm, sqls: gs, deals: gd, revenue: gr };
  }

  weeklyFunnel.push({ week, leads, mqls, sqls, closedWon, closedRevenue, byChannel, byGeo });

  // Stripe
  const stripeTotal = round(rf(3000, 6000) * sm);
  const typeShares = { subscription: 0.5, prepaid: 0.25, invoice: 0.15, "one-time": 0.1 };
  const typeRevDist = distribute(stripeTotal, typeShares);
  const totalCount = ri(30, 70);
  const typeCountDist = distribute(totalCount, typeShares, true);
  const byType = {};
  for (const t of Object.keys(typeShares)) {
    byType[t] = { count: typeCountDist[t], revenue: typeRevDist[t] };
  }
  const curShares = { EUR: 0.8, USD: 0.15, GBP: 0.05 };
  const curRevDist = distribute(stripeTotal, curShares);
  const curCountDist = distribute(totalCount, curShares, true);
  const byCurrency = {};
  for (const c of Object.keys(curShares)) {
    byCurrency[c] = { count: curCountDist[c], revenue: curRevDist[c] };
  }
  weeklyStripe.push({ week, totalRevenue: stripeTotal, count: totalCount, byType, byCurrency });

  // Subscriptions
  const subTotal = ri(3, 8);
  const monthlyRaw = ri(Math.round(subTotal * 0.6), Math.round(subTotal * 0.8));
  const monthlySubs = Math.max(0, monthlyRaw);
  const yearlySubs = Math.max(0, subTotal - monthlySubs);
  weeklySubscriptions.push({ week, total: subTotal, monthly: monthlySubs, yearly: yearlySubs });

  // GA4
  const formSubmissions = ri(Math.round(20 * sm), Math.round(40 * sm));
  const purchases = ri(Math.round(50 * sm), Math.round(100 * sm));
  const purchaseValue = round(rf(2000, 5000) * sm);
  weeklyGa4.push({ week, formSubmissions, purchases, purchaseValue });
}

// Aggregate monthly
const monthlyMap = new Map();
for (let w = 0; w < WEEKS; w++) {
  const month = toMonth(weeklyAds[w].week);
  if (!monthlyMap.has(month)) {
    monthlyMap.set(month, {
      month, adsCost: 0, mmCost: 0, hmCost: 0,
      leads: 0, mqls: 0, sqls: 0, closedWon: 0, closedRevenue: 0, stripeRevenue: 0,
    });
  }
  const m = monthlyMap.get(month);
  m.adsCost += weeklyAds[w].totalCost;
  m.mmCost += weeklyAds[w].mmCost;
  m.hmCost += weeklyAds[w].hmCost;
  m.leads += weeklyFunnel[w].leads;
  m.mqls += weeklyFunnel[w].mqls;
  m.sqls += weeklyFunnel[w].sqls;
  m.closedWon += weeklyFunnel[w].closedWon;
  m.closedRevenue += weeklyFunnel[w].closedRevenue;
  m.stripeRevenue += weeklyStripe[w].totalRevenue;
}

const monthly = [...monthlyMap.values()].map((m) => {
  m.adsCost = round(m.adsCost);
  m.mmCost = round(m.mmCost);
  m.hmCost = round(m.hmCost);
  m.closedRevenue = round(m.closedRevenue);
  m.stripeRevenue = round(m.stripeRevenue);
  m.leadToMql = m.leads ? round(m.mqls / m.leads, 4) : 0;
  m.mqlToSql = m.mqls ? round(m.sqls / m.mqls, 4) : 0;
  m.mqlToDeal = m.mqls ? round(m.closedWon / m.mqls, 4) : 0;
  m.sqlToDeal = m.sqls ? round(m.closedWon / m.sqls, 4) : 0;
  m.dealAOV = m.closedWon ? round(m.closedRevenue / m.closedWon) : 0;
  return m;
});

// KPIs
const totalLeads = weeklyFunnel.reduce((s, w) => s + w.leads, 0);
const totalMQLs = weeklyFunnel.reduce((s, w) => s + w.mqls, 0);
const totalSQLs = weeklyFunnel.reduce((s, w) => s + w.sqls, 0);
const totalDeals = weeklyFunnel.reduce((s, w) => s + w.closedWon, 0);
const totalDealRevenue = round(weeklyFunnel.reduce((s, w) => s + w.closedRevenue, 0));
const totalStripeRevenue = round(weeklyStripe.reduce((s, w) => s + w.totalRevenue, 0));
const totalAdsCost = round(weeklyAds.reduce((s, w) => s + w.totalCost, 0));
const roas = round((totalDealRevenue + totalStripeRevenue) / totalAdsCost, 2);

// Assemble & write
const data = {
  updatedAt: new Date().toISOString(),
  dateRange: { start: "2025-02-17", end: "2026-02-16" },
  kpis: { totalLeads, totalMQLs, totalSQLs, totalDeals, totalDealRevenue, totalStripeRevenue, totalAdsCost, roas },
  monthly,
  weekly: { ads: weeklyAds, funnel: weeklyFunnel, stripe: weeklyStripe, subscriptions: weeklySubscriptions, ga4: weeklyGa4 },
};

writeFileSync(OUTPUT_PATH, JSON.stringify(data, null, 2), "utf-8");

console.log("Data written to " + OUTPUT_PATH);
console.log("");
console.log("KPIs:");
console.log("  Total Leads:          " + totalLeads);
console.log("  Total MQLs:           " + totalMQLs);
console.log("  Total SQLs:           " + totalSQLs);
console.log("  Total Deals:          " + totalDeals);
console.log("  Total Deal Revenue:   EUR " + totalDealRevenue.toLocaleString());
console.log("  Total Stripe Revenue: EUR " + totalStripeRevenue.toLocaleString());
console.log("  Total Ads Cost:       EUR " + totalAdsCost.toLocaleString());
console.log("  ROAS:                 " + roas + "x");
console.log("");
console.log("Months:    " + monthly.length);
console.log("Weeks:     " + WEEKS);
console.log("Ads rows:  " + weeklyAds.length);
console.log("Funnel:    " + weeklyFunnel.length);
console.log("Stripe:    " + weeklyStripe.length);
console.log("Subs:      " + weeklySubscriptions.length);
console.log("GA4:       " + weeklyGa4.length);
