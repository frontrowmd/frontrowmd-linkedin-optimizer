'use strict';

// ============================================================
// Agent 4: LinkedIn Campaign Optimizer & Cost Reduction Agent
// FrontrowMD Marketing Operations
// ============================================================
// Data Sources: Windsor.ai (LinkedIn campaign data) + HubSpot (qualification outcomes)
// Outputs: Slack summary, Email report, HTML dashboard, .txt analysis
// Run: node linkedin_optimizer.js
// ============================================================

require('dotenv').config();
const fetch = require('node-fetch');
const nodemailer = require('nodemailer');
const fs = require('fs');
const path = require('path');

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const WINDSOR_API_KEY = process.env.WINDSOR_API_KEY;
const HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN;
const SLACK_WEBHOOK = process.env.SLACK_WEBHOOK;
const EMAIL_FROM = process.env.EMAIL_FROM;
const EMAIL_PASS = process.env.EMAIL_PASS;
const EMAIL_TO = process.env.EMAIL_TO;
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GITHUB_OWNER = process.env.GITHUB_OWNER;
const GITHUB_REPO = process.env.GITHUB_REPO_LI || process.env.GITHUB_REPO;

// LinkedIn monthly budget (hardcoded per Agent 6 reference)
const LI_MONTHLY_BUDGET = 35000;

// CPD benchmarks (update based on your historical data)
const CPD_TARGET = 150;           // Target CPD (qualified) – current is ~$200
const CPD_BENCHMARK_META = 80;    // Meta CPD reference point
const CPD_BENCHMARK_TIKTOK = 60;  // TikTok CPD reference
const CPD_BENCHMARK_GOOGLE = 100; // Google CPD reference

// Qualification thresholds
const DISQUAL_ALERT_THRESHOLD = 0.45; // Alert if disqual rate > 45%
const DISQUAL_WARN_THRESHOLD = 0.35;  // Warn if disqual rate > 35%
const CTR_WARN_THRESHOLD = 0.005;     // 0.5% CTR warn for LinkedIn
const CPM_ALERT_THRESHOLD = 80;       // $80 CPM alert

// ─── DATE HELPERS ──────────────────────────────────────────────────────────────

function toDateStr(d) {
  return d.toISOString().split('T')[0];
}

function addDays(d, n) {
  const r = new Date(d);
  r.setUTCDate(r.getUTCDate() + n);
  return r;
}

function getWindows() {
  const now = new Date();
  // Windsor caps at yesterday
  const yesterday = addDays(now, -1);
  const d7start = addDays(now, -7);
  const d30start = addDays(now, -30);
  const mtdStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  // Prior month
  const prevMonthEnd = addDays(mtdStart, -1);
  const prevMonthStart = new Date(Date.UTC(prevMonthEnd.getUTCFullYear(), prevMonthEnd.getUTCMonth(), 1));

  return {
    yesterday: { from: toDateStr(yesterday), to: toDateStr(yesterday), label: 'Yesterday' },
    d7:        { from: toDateStr(d7start),   to: toDateStr(yesterday), label: 'Last 7 Days' },
    d30:       { from: toDateStr(d30start),  to: toDateStr(yesterday), label: 'Last 30 Days' },
    mtd:       { from: toDateStr(mtdStart),  to: toDateStr(yesterday), label: 'Month to Date' },
    prevMonth: { from: toDateStr(prevMonthStart), to: toDateStr(prevMonthEnd), label: 'Prior Month' },
  };
}

function toMs(dateStr, endOfDay = false) {
  const d = new Date(dateStr + 'T00:00:00.000Z');
  return endOfDay ? d.getTime() + 86399999 : d.getTime();
}

// ─── WINDSOR.AI FETCHER ────────────────────────────────────────────────────────

async function windsorFetch(params, attempt = 0) {
  const url = new URL('https://connectors.windsor.ai/all');
  for (const [k, v] of Object.entries({ api_key: WINDSOR_API_KEY, page_size: 5000, ...params })) {
    url.searchParams.set(k, String(v));
  }
  try {
    const res = await fetch(url.toString());
    if (!res.ok) throw new Error(`Windsor HTTP ${res.status}`);
    const json = await res.json();
    return json.data || [];
  } catch (err) {
    if (attempt < 3) {
      await sleep(1000 * Math.pow(2, attempt));
      return windsorFetch(params, attempt + 1);
    }
    console.error('Windsor fetch failed:', err.message);
    return [];
  }
}

// Fetch LinkedIn data with campaign dimension
async function fetchLinkedInData(from, to) {
  return windsorFetch({
    date_from: from,
    date_to: to,
    fields: 'campaign_name,spend,clicks,impressions,ctr,cpm,conversions_hubspot_meeting_booked,date',
    connectors: 'linkedin',
  });
}

// Fetch all-channel data for comparison
async function fetchAllChannelData(from, to) {
  return windsorFetch({
    date_from: from,
    date_to: to,
    fields: 'datasource,spend,clicks,impressions,ctr,cpm,conversions_hubspot_meeting_booked',
  });
}

// Fetch GA4 for website funnel context
async function fetchGA4(from, to) {
  const rows = await windsorFetch({
    date_from: from,
    date_to: to,
    fields: 'users,sessions,conversions_click_schedule_demo_button,conversions_hubspot_meeting_booked',
    connectors: 'googleanalytics4',
  });
  return rows.reduce((acc, r) => ({
    users: (acc.users || 0) + num(r.users),
    sessions: (acc.sessions || 0) + num(r.sessions),
    demoClicks: (acc.demoClicks || 0) + num(r.conversions_click_schedule_demo_button),
    demos: (acc.demos || 0) + num(r.conversions_hubspot_meeting_booked),
  }), {});
}

// ─── HUBSPOT FETCHER ───────────────────────────────────────────────────────────

async function hsSearch(objectType, payload) {
  const results = [];
  let after = undefined;
  let pages = 0;
  while (pages < 50) {
    const body = { ...payload, limit: 100 };
    if (after) body.after = after;
    const res = await fetch(`https://api.hubapi.com/crm/v3/objects/${objectType}/search`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.text();
      console.error(`HubSpot search error (${objectType}): ${res.status} ${err}`);
      break;
    }
    const data = await res.json();
    results.push(...(data.results || []));
    if (data.paging?.next?.after) {
      after = data.paging.next.after;
      pages++;
      await sleep(120); // rate limit
    } else break;
  }
  return results;
}

// Fetch all HubSpot demo data for the widest window, slice in memory
async function fetchAllHubSpotData(fromStr, toStr) {
  const fromMs = toMs(fromStr);
  const toMs_ = toMs(toStr, true);

  // Demos booked (contacts with date_demo_booked in range)
  const contacts = await hsSearch('contacts', {
    filterGroups: [{
      filters: [
        { propertyName: 'date_demo_booked', operator: 'GTE', value: String(fromMs) },
        { propertyName: 'date_demo_booked', operator: 'LTE', value: String(toMs_) },
      ]
    }],
    properties: ['date_demo_booked', 'demo_status', 'disqualification_reason',
                 'hs_analytics_source', 'hs_analytics_source_data_1', 'hs_lead_status',
                 'lifecyclestage'],
    sorts: [{ propertyName: 'date_demo_booked', direction: 'ASCENDING' }],
  });

  // Closed won deals
  const deals = await hsSearch('deals', {
    filterGroups: [{
      filters: [
        { propertyName: 'closedate', operator: 'GTE', value: String(fromMs) },
        { propertyName: 'closedate', operator: 'LTE', value: String(toMs_) },
        { propertyName: 'dealstage', operator: 'EQ', value: 'closedwon' },
      ]
    }],
    properties: ['closedate', 'amount', 'dealstage'],
  });

  return { contacts, deals };
}

// Slice contacts/deals to a sub-window
function sliceWindow(data, fromStr, toStr) {
  const fromMs = toMs(fromStr);
  const toMs_ = toMs(toStr, true);

  const contacts = data.contacts.filter(c => {
    const v = parseInt(c.properties?.date_demo_booked || '0', 10);
    return v >= fromMs && v <= toMs_;
  });
  const deals = data.deals.filter(d => {
    const v = new Date(d.properties?.closedate || 0).getTime();
    return v >= fromMs && v <= toMs_;
  });
  return { contacts, deals };
}

// Build pipeline metrics from sliced data
function buildPipelineMetrics(sliced) {
  const { contacts, deals } = sliced;
  const demosBooked = contacts.length;
  const demosHappened = contacts.filter(c => c.properties?.demo_status === 'Happened').length;
  const noShow = contacts.filter(c => c.properties?.demo_status === 'No Show').length;
  const cancelled = contacts.filter(c => c.properties?.demo_status === 'Cancelled').length;
  const disqualified = contacts.filter(c => c.properties?.disqualification_reason).length;

  // Disqualification reason breakdown
  const disqualReasons = {};
  contacts.forEach(c => {
    const r = c.properties?.disqualification_reason;
    if (r) disqualReasons[r] = (disqualReasons[r] || 0) + 1;
  });

  const closedWon = deals.length;
  const mrr = deals.reduce((s, d) => s + num(d.properties?.amount), 0);
  const showRate = demosBooked > 0 ? demosHappened / demosBooked : 0;
  const disqualRate = demosBooked > 0 ? disqualified / demosBooked : 0;

  return { demosBooked, demosHappened, noShow, cancelled, disqualified,
           disqualReasons, closedWon, mrr, showRate, disqualRate };
}

// ─── CHANNEL AGGREGATION ───────────────────────────────────────────────────────

function aggregateChannels(rows) {
  const channels = {};
  for (const r of rows) {
    const ds = (r.datasource || 'unknown').toLowerCase();
    if (!channels[ds]) channels[ds] = { spend: 0, clicks: 0, impressions: 0, demos: 0 };
    channels[ds].spend += num(r.spend);
    channels[ds].clicks += num(r.clicks);
    channels[ds].impressions += num(r.impressions);
    channels[ds].demos += num(r.conversions_hubspot_meeting_booked);
  }
  // Compute derived
  for (const ch of Object.values(channels)) {
    ch.ctr = ch.impressions > 0 ? ch.clicks / ch.impressions : 0;
    ch.cpm = ch.impressions > 0 ? (ch.spend / ch.impressions) * 1000 : 0;
    ch.cpd = ch.demos > 0 ? ch.spend / ch.demos : 0;
    ch.cpc = ch.clicks > 0 ? ch.spend / ch.clicks : 0;
  }
  return channels;
}

// Aggregate LinkedIn by campaign
function aggregateByCampaign(rows) {
  const campaigns = {};
  for (const r of rows) {
    const name = r.campaign_name || 'Unknown Campaign';
    if (!campaigns[name]) campaigns[name] = { spend: 0, clicks: 0, impressions: 0, demos: 0 };
    campaigns[name].spend += num(r.spend);
    campaigns[name].clicks += num(r.clicks);
    campaigns[name].impressions += num(r.impressions);
    campaigns[name].demos += num(r.conversions_hubspot_meeting_booked);
  }
  for (const c of Object.values(campaigns)) {
    c.ctr = c.impressions > 0 ? c.clicks / c.impressions : 0;
    c.cpm = c.impressions > 0 ? (c.spend / c.impressions) * 1000 : 0;
    c.cpd = c.demos > 0 ? c.spend / c.demos : 0;
    c.cpc = c.clicks > 0 ? c.spend / c.clicks : 0;
  }
  return campaigns;
}

// ─── INTELLIGENCE ENGINE ───────────────────────────────────────────────────────

function buildLinkedInIntelligence({ liData30, allChannels30, pipeline30, pipelinePrevMonth, budgetPaced }) {
  const alerts = [], warnings = [], opportunities = [], wins = [];

  const li = liData30;
  const totalSpend = li.spend || 0;
  const totalDemos = li.demos || 0;
  const cpd = li.cpd || 0;
  const ctr = li.ctr || 0;
  const cpm = li.cpm || 0;

  // CPD vs target
  if (cpd > CPD_TARGET * 1.5) {
    alerts.push(`🚨 LinkedIn CPD is ${fmt$(cpd)} — ${Math.round(((cpd - CPD_TARGET) / CPD_TARGET) * 100)}% above target of ${fmt$(CPD_TARGET)}. Immediate audience/bid review recommended.`);
  } else if (cpd > CPD_TARGET * 1.2) {
    warnings.push(`⚠️ LinkedIn CPD at ${fmt$(cpd)} is above target (${fmt$(CPD_TARGET)}). Review top-spending campaigns for efficiency.`);
  } else if (cpd > 0 && cpd <= CPD_TARGET) {
    wins.push(`✅ LinkedIn CPD at ${fmt$(cpd)} is at or below target of ${fmt$(CPD_TARGET)}.`);
  }

  // CPD vs other channels
  const metaCPD = allChannels30.facebook?.cpd || 0;
  const googleCPD = allChannels30.google_ads?.cpd || 0;
  if (cpd > 0 && metaCPD > 0 && cpd > metaCPD * 2) {
    const savings = totalDemos > 0 ? Math.round((cpd - metaCPD) * totalDemos) : 0;
    warnings.push(`⚠️ LinkedIn CPD (${fmt$(cpd)}) is ${Math.round(cpd/metaCPD)}x Meta CPD (${fmt$(metaCPD)}). Shifting 10% of LinkedIn budget to Meta could save ~${fmt$(savings)}/mo.`);
  }
  if (cpd > 0 && googleCPD > 0) {
    opportunities.push(`💡 Google CPD is ${fmt$(googleCPD)} vs LinkedIn ${fmt$(cpd)}. Consider testing budgets on Google Search for qualified B2B intent.`);
  }

  // CTR
  if (ctr > 0 && ctr < CTR_WARN_THRESHOLD) {
    warnings.push(`⚠️ LinkedIn CTR at ${fmtPct(ctr)} is below 0.5% benchmark. Creative refresh or audience expansion likely needed.`);
  } else if (ctr >= 0.008) {
    wins.push(`✅ LinkedIn CTR at ${fmtPct(ctr)} is strong (benchmark: 0.5%).`);
  }

  // CPM
  if (cpm > CPM_ALERT_THRESHOLD) {
    alerts.push(`🚨 LinkedIn CPM at ${fmt$(cpm)} is very high. Consider narrowing or expanding audiences to reset auction dynamics.`);
  } else if (cpm > 50) {
    warnings.push(`⚠️ LinkedIn CPM at ${fmt$(cpm)} is elevated. Audience fatigue or narrow targeting may be driving costs up.`);
  }

  // Disqualification rate
  const disqual = pipeline30.disqualRate || 0;
  if (disqual > DISQUAL_ALERT_THRESHOLD) {
    const wastedSpend = Math.round(totalSpend * disqual);
    alerts.push(`🚨 Disqualification rate at ${fmtPct(disqual)} of demos. Estimated ${fmt$(wastedSpend)}/mo in LinkedIn spend wasted on unqualified leads. Exclusion audiences recommended.`);
  } else if (disqual > DISQUAL_WARN_THRESHOLD) {
    warnings.push(`⚠️ Disqualification rate at ${fmtPct(disqual)}. Review HubSpot disqual reasons to identify targeting exclusion patterns.`);
  } else if (disqual > 0 && disqual < 0.25) {
    wins.push(`✅ Disqualification rate at ${fmtPct(disqual)} — below 25% threshold.`);
  }

  // Budget pacing
  if (budgetPaced > 1.0) {
    alerts.push(`🚨 LinkedIn spend is over-pacing (${fmtPct(budgetPaced)} of monthly budget consumed). Reduce daily caps to avoid overspend.`);
  } else if (budgetPaced > 0.85) {
    warnings.push(`⚠️ LinkedIn spend pacing at ${fmtPct(budgetPaced)} of monthly budget — on track but monitor closely.`);
  } else if (budgetPaced < 0.4) {
    opportunities.push(`💡 LinkedIn is under-pacing at ${fmtPct(budgetPaced)} of monthly budget. If CPD is favorable, consider increasing daily budgets to capture volume.`);
  }

  // Pipeline trend
  const currDemos = pipeline30.demosBooked || 0;
  const prevDemos = pipelinePrevMonth.demosBooked || 0;
  if (prevDemos > 0) {
    const delta = (currDemos - prevDemos) / prevDemos;
    if (delta < -0.2) {
      warnings.push(`⚠️ LinkedIn pipeline volume is down ${Math.abs(Math.round(delta * 100))}% vs prior month (${currDemos} vs ${prevDemos} demos). Investigate audience saturation.`);
    } else if (delta > 0.2) {
      wins.push(`✅ LinkedIn pipeline volume up ${Math.round(delta * 100)}% vs prior month (${currDemos} vs ${prevDemos} demos).`);
    }
  }

  // Show rate
  const showRate = pipeline30.showRate || 0;
  if (showRate < 0.55) {
    warnings.push(`⚠️ Demo show rate at ${fmtPct(showRate)}. Consider reminder sequences or qualification gate on booking page to improve quality.`);
  } else if (showRate >= 0.75) {
    wins.push(`✅ Demo show rate at ${fmtPct(showRate)} — healthy lead quality signal.`);
  }

  // Disqual reasons actionability
  const topDisqual = Object.entries(pipeline30.disqualReasons || {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3);
  if (topDisqual.length > 0 && topDisqual[0][1] >= 3) {
    opportunities.push(`💡 Top disqualification reason: "${topDisqual[0][0]}" (${topDisqual[0][1]} demos). Build audience exclusion list to block this segment pre-click.`);
  }

  return { alerts, warnings, opportunities, wins };
}

// ─── CAMPAIGN RECOMMENDATIONS ──────────────────────────────────────────────────

function buildCampaignRecommendations(campaigns30, pipeline30) {
  const recs = [];
  const sorted = Object.entries(campaigns30).sort((a, b) => b[1].spend - a[1].spend);

  if (sorted.length === 0) {
    recs.push({ type: 'info', text: 'No campaign-level data available for the 30-day window.' });
    return recs;
  }

  // Top spenders with poor CPD
  const totalSpend = sorted.reduce((s, [, v]) => s + v.spend, 0);
  for (const [name, c] of sorted) {
    const share = totalSpend > 0 ? c.spend / totalSpend : 0;
    if (share > 0.3 && c.cpd > CPD_TARGET * 1.5 && c.demos < 3) {
      recs.push({
        type: 'pause',
        campaign: name,
        text: `PAUSE / REVIEW: "${name}" is consuming ${fmtPct(share)} of LinkedIn spend (${fmt$(c.spend)}) with only ${c.demos} demos (CPD: ${fmt$(c.cpd)}). Recommend pausing and reallocating budget.`,
      });
    } else if (share > 0.15 && c.cpd > CPD_TARGET * 1.2) {
      recs.push({
        type: 'reduce',
        campaign: name,
        text: `REDUCE BUDGET: "${name}" CPD of ${fmt$(c.cpd)} is above target. Reduce daily spend by 20-30% and monitor quality.`,
      });
    }

    // Winners to scale
    if (c.cpd > 0 && c.cpd < CPD_TARGET * 0.8 && c.demos >= 3) {
      recs.push({
        type: 'scale',
        campaign: name,
        text: `SCALE: "${name}" has a CPD of ${fmt$(c.cpd)} — below target. Increase budget by 20-30% to capture more volume.`,
      });
    }

    // CTR red flags
    if (c.ctr < 0.003 && c.impressions > 5000) {
      recs.push({
        type: 'creative',
        campaign: name,
        text: `REFRESH CREATIVE: "${name}" CTR is ${fmtPct(c.ctr)} with ${fmtNum(c.impressions)} impressions. Ad creative is fatigued — rotate new variants.`,
      });
    }
  }

  // Concentration risk
  if (sorted.length > 0) {
    const topShare = totalSpend > 0 ? sorted[0][1].spend / totalSpend : 0;
    if (topShare > 0.6) {
      recs.push({
        type: 'risk',
        text: `CONCENTRATION RISK: Top campaign "${sorted[0][0]}" absorbs ${fmtPct(topShare)} of LinkedIn budget. Diversify into 2-3 parallel campaigns to reduce risk.`,
      });
    }
  }

  return recs;
}

// Audience targeting recommendations (static playbook)
function buildAudiencePlaybook(pipeline30) {
  const disqualReasons = pipeline30.disqualReasons || {};
  const topDisquals = Object.entries(disqualReasons).sort((a, b) => b[1] - a[1]).slice(0, 5);

  return {
    exclusions: [
      'Company age < 6 months (pre-revenue brands)',
      'Job title contains: "Student", "Intern", "Freelance"',
      'Company size: 1-5 employees (solo operators)',
      ...topDisquals.map(([r]) => `Disqual pattern: "${r}" — build audience exclusion`),
    ],
    layeringStrategies: [
      'Job title (e.g., CEO, CMO, Marketing Director) + Company size (11-200) + Industry (Health & Wellness)',
      'Retargeting: Engaged with LinkedIn page + visited frontrowmd.com (matched audience)',
      'Lookalike: Upload closed-won customer list for LinkedIn Matched Audience expansion',
      'Interest-based: "Marketing & Advertising" + "E-Commerce" + "Health & Wellness"',
      'Account-based: Upload ICP company list for Account Targeting in Campaign Manager',
    ],
    bidStrategy: [
      'Test: "Maximum Delivery" (auto bid) vs. "Target Cost" at $120-140 CPD',
      'If CPM > $60: Switch from "Reach" objective to "Lead Gen Form" to improve lead quality',
      'For retargeting campaigns: Manual CPC with $8-12 bid cap (smaller audience, higher intent)',
      'For prospecting: Start with auto bid to gather data, then switch to target cost once 30+ conversions',
    ],
  };
}

// ─── REPORT BUILDERS ──────────────────────────────────────────────────────────

function buildTextReport({ windows, liData, allChannels, pipelines, intelligence, campaignRecs, audiencePlaybook, campaigns30 }) {
  const w = windows;
  const lines = [];

  lines.push('='.repeat(70));
  lines.push('FRONTROWMD — LINKEDIN CAMPAIGN OPTIMIZER REPORT');
  lines.push(`Generated: ${new Date().toLocaleString('en-US', { timeZone: 'America/New_York' })} ET`);
  lines.push(`Period: ${w.d30.from} → ${w.d30.to} (primary: 30-day)`);
  lines.push('='.repeat(70));

  // ── EXECUTIVE SUMMARY ──
  lines.push('\n── EXECUTIVE SUMMARY ──────────────────────────────────────────────────\n');
  const li30 = liData.d30;
  const li7 = liData.d7;
  const pipeline = pipelines.d30;
  lines.push(`LinkedIn 30-Day Performance:`);
  lines.push(`  Total Spend:    ${fmt$(li30.spend)}`);
  lines.push(`  Total Demos:    ${li30.demos}`);
  lines.push(`  Cost Per Demo:  ${fmt$(li30.cpd)} (target: ${fmt$(CPD_TARGET)})`);
  lines.push(`  CTR:            ${fmtPct(li30.ctr)}`);
  lines.push(`  CPM:            ${fmt$(li30.cpm)}`);
  lines.push(`  CPC:            ${fmt$(li30.cpc)}`);
  lines.push('');
  lines.push(`Pipeline (30-day, all channels via HubSpot):`);
  lines.push(`  Demos Booked:   ${pipeline.demosBooked}`);
  lines.push(`  Demos Happened: ${pipeline.demosHappened}  (Show Rate: ${fmtPct(pipeline.showRate)})`);
  lines.push(`  Disqualified:   ${pipeline.disqualified}  (${fmtPct(pipeline.disqualRate)})`);
  lines.push(`  Closed Won:     ${pipeline.closedWon}`);
  lines.push(`  MRR:            ${fmt$(pipeline.mrr)}`);

  // ── INTELLIGENCE ──
  lines.push('\n── INTELLIGENCE ENGINE ─────────────────────────────────────────────────\n');
  if (intelligence.alerts.length > 0) {
    lines.push('ALERTS:');
    intelligence.alerts.forEach(a => lines.push(`  ${a}`));
  }
  if (intelligence.warnings.length > 0) {
    lines.push('\nWEAKNESSES / WARNINGS:');
    intelligence.warnings.forEach(w => lines.push(`  ${w}`));
  }
  if (intelligence.opportunities.length > 0) {
    lines.push('\nOPPORTUNITIES:');
    intelligence.opportunities.forEach(o => lines.push(`  ${o}`));
  }
  if (intelligence.wins.length > 0) {
    lines.push('\nWINS:');
    intelligence.wins.forEach(w => lines.push(`  ${w}`));
  }

  // ── CAMPAIGN BREAKDOWN ──
  lines.push('\n── CAMPAIGN BREAKDOWN (30-DAY) ─────────────────────────────────────────\n');
  const sortedCamps = Object.entries(campaigns30).sort((a, b) => b[1].spend - a[1].spend);
  if (sortedCamps.length === 0) {
    lines.push('  No campaign-level data available.');
  } else {
    lines.push(padStr('Campaign', 40) + padStr('Spend', 12) + padStr('Demos', 8) + padStr('CPD', 10) + padStr('CTR', 8) + 'CPM');
    lines.push('-'.repeat(90));
    for (const [name, c] of sortedCamps) {
      lines.push(
        padStr(name.substring(0, 39), 40) +
        padStr(fmt$(c.spend), 12) +
        padStr(String(c.demos), 8) +
        padStr(c.cpd > 0 ? fmt$(c.cpd) : '-', 10) +
        padStr(fmtPct(c.ctr), 8) +
        fmt$(c.cpm)
      );
    }
  }

  // ── CAMPAIGN RECOMMENDATIONS ──
  lines.push('\n── CAMPAIGN ACTION ITEMS ───────────────────────────────────────────────\n');
  if (campaignRecs.length === 0) {
    lines.push('  No specific campaign actions flagged at this time.');
  } else {
    campaignRecs.forEach(r => lines.push(`  [${(r.type || 'info').toUpperCase()}] ${r.text}`));
  }

  // ── CHANNEL COMPARISON ──
  lines.push('\n── CROSS-CHANNEL CPD COMPARISON (30-DAY) ──────────────────────────────\n');
  const channelMap = {
    linkedin: 'LinkedIn',
    facebook: 'Meta',
    tiktok: 'TikTok',
    google_ads: 'Google Ads',
    youtube: 'YouTube',
  };
  for (const [ds, label] of Object.entries(channelMap)) {
    const ch = allChannels.d30[ds];
    if (!ch || ch.spend === 0) continue;
    const cpdStr = ch.cpd > 0 ? fmt$(ch.cpd) : 'N/A';
    const vsLi = li30.cpd > 0 && ch.cpd > 0 ? ` (${ch.cpd < li30.cpd ? '' : '+'}${Math.round(((ch.cpd - li30.cpd) / li30.cpd) * 100)}% vs LI)` : '';
    lines.push(`  ${padStr(label, 14)} Spend: ${padStr(fmt$(ch.spend), 12)} Demos: ${padStr(String(ch.demos), 6)} CPD: ${cpdStr}${vsLi}`);
  }

  // ── DISQUALIFICATION BREAKDOWN ──
  lines.push('\n── DISQUALIFICATION BREAKDOWN (30-DAY) ─────────────────────────────────\n');
  const dq = pipelines.d30.disqualReasons;
  if (Object.keys(dq).length === 0) {
    lines.push('  No disqualification data available.');
  } else {
    Object.entries(dq).sort((a, b) => b[1] - a[1]).forEach(([reason, count]) => {
      lines.push(`  ${padStr(reason, 40)} ${count} demos`);
    });
  }

  // ── AUDIENCE PLAYBOOK ──
  lines.push('\n── AUDIENCE & TARGETING PLAYBOOK ───────────────────────────────────────\n');
  lines.push('Recommended Exclusions:');
  audiencePlaybook.exclusions.forEach(e => lines.push(`  - ${e}`));
  lines.push('\nAudience Layering Strategies to Test:');
  audiencePlaybook.layeringStrategies.forEach(s => lines.push(`  - ${s}`));
  lines.push('\nBid Strategy Recommendations:');
  audiencePlaybook.bidStrategy.forEach(b => lines.push(`  - ${b}`));

  lines.push('\n' + '='.repeat(70));
  lines.push('END OF REPORT');
  lines.push('='.repeat(70));

  return lines.join('\n');
}

function buildSlackSummary({ liData, pipelines, intelligence, windows, dashboardUrl }) {
  const li30 = liData.d30;
  const li7 = liData.d7;
  const p30 = pipelines.d30;

  const cpdVsTarget = li30.cpd > 0
    ? `${li30.cpd > CPD_TARGET ? '🔴' : '🟢'} ${fmt$(li30.cpd)} (target: ${fmt$(CPD_TARGET)})`
    : 'No data';

  const totalAlerts = intelligence.alerts.length;
  const totalWins = intelligence.wins.length;
  const statusEmoji = totalAlerts > 0 ? '🔴' : intelligence.warnings.length > 0 ? '🟡' : '🟢';

  let msg = `*🔗 LinkedIn Campaign Optimizer — ${windows.d30.label}*\n`;
  msg += `${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })}\n\n`;

  msg += `*LinkedIn Performance (30 Days)*\n`;
  msg += `• Spend: ${fmt$(li30.spend)} | Demos: ${li30.demos} | CPD: ${cpdVsTarget}\n`;
  msg += `• CTR: ${fmtPct(li30.ctr)} | CPM: ${fmt$(li30.cpm)} | CPC: ${fmt$(li30.cpc)}\n\n`;

  msg += `*Pipeline (30 Days)*\n`;
  msg += `• Booked: ${p30.demosBooked} | Happened: ${p30.demosHappened} (${fmtPct(p30.showRate)} show rate)\n`;
  msg += `• Disqualified: ${p30.disqualified} (${fmtPct(p30.disqualRate)}) | Closed Won: ${p30.closedWon}\n\n`;

  msg += `*${statusEmoji} Intelligence: ${totalAlerts} Alerts | ${intelligence.warnings.length} Warnings | ${totalWins} Wins*\n`;

  if (intelligence.alerts.length > 0) {
    intelligence.alerts.slice(0, 2).forEach(a => { msg += `${a}\n`; });
  }
  if (intelligence.opportunities.length > 0) {
    msg += `\n*Top Opportunity:*\n${intelligence.opportunities[0]}\n`;
  }

  msg += `\n_Run \`node linkedin_optimizer.js\` for full report + dashboard._`;
  if (dashboardUrl) msg += `\n<${dashboardUrl}|View Full Dashboard →>`;
  return msg;
}

// ─── HTML DASHBOARD ────────────────────────────────────────────────────────────

function buildDashboard({ liData, allChannels, pipelines, intelligence, campaignRecs, audiencePlaybook, campaigns30, windows }) {
  const li30 = liData.d30;
  const li7 = liData.d7;
  const p30 = pipelines.d30;
  const prevP = pipelines.prevMonth;

  const cpdColor = li30.cpd > CPD_TARGET * 1.3 ? '#EF4444' : li30.cpd > CPD_TARGET ? '#F59E0B' : '#72A4BF';

  const campaignRows = Object.entries(campaigns30)
    .sort((a, b) => b[1].spend - a[1].spend)
    .map(([name, c]) => {
      const cpdBg = c.cpd > CPD_TARGET * 1.3 ? 'rgba(239,68,68,0.1)' : c.cpd < CPD_TARGET * 0.8 && c.cpd > 0 ? 'rgba(34,197,94,0.1)' : 'transparent';
      return `<tr>
        <td>${escHtml(name)}</td>
        <td>${fmt$(c.spend)}</td>
        <td>${c.demos}</td>
        <td style="background:${cpdBg}; font-weight:bold;">${c.cpd > 0 ? fmt$(c.cpd) : '-'}</td>
        <td>${fmtPct(c.ctr)}</td>
        <td>${fmt$(c.cpm)}</td>
        <td>${fmt$(c.cpc)}</td>
      </tr>`;
    }).join('');

  const channelCompRows = Object.entries({
    linkedin: 'LinkedIn', facebook: 'Meta', tiktok: 'TikTok', google_ads: 'Google Ads', youtube: 'YouTube'
  }).map(([ds, label]) => {
    const ch = allChannels.d30[ds];
    if (!ch || ch.spend === 0) return '';
    const isLi = ds === 'linkedin';
    return `<tr ${isLi ? 'class="li-row"' : ''}>
      <td><strong>${label}</strong>${isLi ? ' ◀' : ''}</td>
      <td>${fmt$(ch.spend)}</td>
      <td>${ch.demos}</td>
      <td style="font-weight:${isLi ? 'bold' : 'normal'}; color:${ch.cpd > CPD_TARGET ? (isLi ? '#EF4444' : '#9CA3AF') : '#22C55E'}">${ch.cpd > 0 ? fmt$(ch.cpd) : '-'}</td>
      <td>${fmtPct(ch.ctr)}</td>
      <td>${fmt$(ch.cpm)}</td>
    </tr>`;
  }).join('');

  const intelligenceHtml = [
    ...intelligence.alerts.map(a => `<div class="intel-item alert">${a}</div>`),
    ...intelligence.warnings.map(w => `<div class="intel-item warning">${w}</div>`),
    ...intelligence.opportunities.map(o => `<div class="intel-item opportunity">${o}</div>`),
    ...intelligence.wins.map(w => `<div class="intel-item win">${w}</div>`),
  ].join('') || '<div class="intel-item win">✅ No issues detected at this time.</div>';

  const recsHtml = campaignRecs.map(r => {
    const colors = { pause: '#EF4444', reduce: '#F59E0B', scale: '#22C55E', creative: '#72A4BF', risk: '#F59E0B', info: '#9CA3AF' };
    const color = colors[r.type || 'info'] || '#9CA3AF';
    return `<div class="rec-item" style="border-left: 3px solid ${color}; padding: 10px 14px; margin-bottom: 8px; background: rgba(0,0,0,0.2); border-radius: 0 8px 8px 0;">
      <span style="color:${color}; font-weight:bold; text-transform:uppercase; font-size:11px;">${r.type || 'info'}</span>
      <div style="margin-top:4px;">${r.text}</div>
    </div>`;
  }).join('') || '<div style="opacity:0.6;">No specific campaign actions at this time.</div>';

  const playbookExclHtml = audiencePlaybook.exclusions.map(e =>
    `<div class="playbook-item">❌ ${e}</div>`).join('');
  const playbookLayerHtml = audiencePlaybook.layeringStrategies.map(s =>
    `<div class="playbook-item">🎯 ${s}</div>`).join('');
  const playbookBidHtml = audiencePlaybook.bidStrategy.map(b =>
    `<div class="playbook-item">⚡ ${b}</div>`).join('');

  const dqHtml = Object.entries(p30.disqualReasons || {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([r, n]) => {
      const pct = p30.demosBooked > 0 ? n / p30.demosBooked : 0;
      const barW = Math.round(pct * 200);
      return `<div style="display:flex; align-items:center; gap:12px; margin-bottom:8px;">
        <div style="width:180px; font-size:13px; opacity:0.9;">${escHtml(r)}</div>
        <div style="width:${barW}px; height:8px; background:#72A4BF; border-radius:4px;"></div>
        <div style="font-size:13px;">${n} (${fmtPct(pct)})</div>
      </div>`;
    }).join('') || '<div style="opacity:0.5;">No disqualification data.</div>';

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>FrontrowMD — LinkedIn Campaign Optimizer</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet"/>
<style>
  *{box-sizing:border-box; margin:0; padding:0;}
  body{font-family:'Libre Baskerville',Georgia,serif; background:linear-gradient(135deg,#020F18 0%,#1D4053 50%,#72A4BF 100%); min-height:100vh; color:#fff; padding:0;}
  .header{background:rgba(2,15,24,0.6); padding:28px 40px; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid rgba(114,164,191,0.2);}
  .logo{font-size:22px; color:#fff; letter-spacing:0.01em;}
  .logo span{font-weight:bold;}
  .header-right{text-align:right; font-size:13px; opacity:0.7;}
  .main{padding:32px 40px; max-width:1400px; margin:0 auto;}
  h2{font-size:18px; font-weight:bold; color:#72A4BF; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:16px;}
  h3{font-size:15px; color:#72A4BF; margin-bottom:12px; font-weight:normal; letter-spacing:0.02em;}
  .kpi-grid{display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:16px; margin-bottom:32px;}
  .kpi-card{background:rgba(23,44,69,0.7); border:1px solid rgba(114,164,191,0.2); border-radius:14px; padding:20px 22px;}
  .kpi-label{font-size:11px; color:#72A4BF; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:6px;}
  .kpi-value{font-size:28px; font-weight:bold; color:#fff; line-height:1.1;}
  .kpi-sub{font-size:12px; margin-top:6px; opacity:0.65;}
  .section{background:rgba(23,44,69,0.5); border:1px solid rgba(114,164,191,0.15); border-radius:14px; padding:24px 28px; margin-bottom:24px;}
  table{width:100%; border-collapse:collapse; font-size:13px;}
  th{color:#72A4BF; text-align:left; padding:8px 10px; border-bottom:1px solid rgba(114,164,191,0.2); font-weight:normal; text-transform:uppercase; letter-spacing:0.04em; font-size:11px;}
  td{padding:9px 10px; border-bottom:1px solid rgba(114,164,191,0.08);}
  tr:last-child td{border-bottom:none;}
  .li-row td{background:rgba(114,164,191,0.08);}
  .intel-item{padding:11px 14px; margin-bottom:8px; border-radius:8px; font-size:13px; line-height:1.5;}
  .intel-item.alert{background:rgba(239,68,68,0.12); border-left:3px solid #EF4444;}
  .intel-item.warning{background:rgba(245,158,11,0.12); border-left:3px solid #F59E0B;}
  .intel-item.opportunity{background:rgba(114,164,191,0.12); border-left:3px solid #72A4BF;}
  .intel-item.win{background:rgba(34,197,94,0.10); border-left:3px solid #22C55E;}
  .two-col{display:grid; grid-template-columns:1fr 1fr; gap:20px;}
  .three-col{display:grid; grid-template-columns:1fr 1fr 1fr; gap:20px;}
  .playbook-item{font-size:13px; padding:8px 0; border-bottom:1px solid rgba(114,164,191,0.1); line-height:1.5;}
  .playbook-item:last-child{border-bottom:none;}
  .budget-bar{background:rgba(114,164,191,0.15); border-radius:4px; height:10px; overflow:hidden; margin-top:8px;}
  .budget-bar-fill{height:100%; border-radius:4px; transition:width 0.3s;}
  footer{text-align:center; padding:24px; font-size:12px; opacity:0.4;}
  @media(max-width:768px){.two-col,.three-col{grid-template-columns:1fr;} .main{padding:20px;}}
</style>
</head>
<body>
<div class="header">
  <div class="logo">☤ frontrow<span>MD</span> — LinkedIn Optimizer</div>
  <div class="header-right">
    Agent 4 · ${windows.d30.from} → ${windows.d30.to}<br/>
    Generated ${new Date().toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'})}
  </div>
</div>

<div class="main">

  <!-- KPI CARDS -->
  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-label">LinkedIn Spend (30d)</div>
      <div class="kpi-value">${fmt$(li30.spend)}</div>
      <div class="kpi-sub">Budget: ${fmt$(LI_MONTHLY_BUDGET)}/mo</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Cost Per Demo</div>
      <div class="kpi-value" style="color:${cpdColor};">${fmt$(li30.cpd)}</div>
      <div class="kpi-sub">Target: ${fmt$(CPD_TARGET)}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Demos (Windsor)</div>
      <div class="kpi-value">${li30.demos}</div>
      <div class="kpi-sub">7d: ${li7.demos}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">CTR</div>
      <div class="kpi-value" style="color:${li30.ctr < CTR_WARN_THRESHOLD ? '#F59E0B' : '#72A4BF'};">${fmtPct(li30.ctr)}</div>
      <div class="kpi-sub">Benchmark: 0.5%</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">CPM</div>
      <div class="kpi-value" style="color:${li30.cpm > CPM_ALERT_THRESHOLD ? '#EF4444' : '#fff'};">${fmt$(li30.cpm)}</div>
      <div class="kpi-sub">CPC: ${fmt$(li30.cpc)}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Disqual Rate (30d)</div>
      <div class="kpi-value" style="color:${p30.disqualRate > DISQUAL_ALERT_THRESHOLD ? '#EF4444' : p30.disqualRate > DISQUAL_WARN_THRESHOLD ? '#F59E0B' : '#22C55E'};">${fmtPct(p30.disqualRate)}</div>
      <div class="kpi-sub">${p30.disqualified} / ${p30.demosBooked} demos</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Show Rate (30d)</div>
      <div class="kpi-value" style="color:${p30.showRate < 0.55 ? '#F59E0B' : '#22C55E'};">${fmtPct(p30.showRate)}</div>
      <div class="kpi-sub">${p30.demosHappened} / ${p30.demosBooked} showed</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Closed Won MRR (30d)</div>
      <div class="kpi-value">${fmt$(p30.mrr)}</div>
      <div class="kpi-sub">${p30.closedWon} deals</div>
    </div>
  </div>

  <!-- INTELLIGENCE ENGINE -->
  <div class="section">
    <h2>Intelligence Engine</h2>
    ${intelligenceHtml}
  </div>

  <!-- CAMPAIGN BREAKDOWN + RECS -->
  <div class="two-col">
    <div class="section">
      <h2>Campaign Breakdown (30d)</h2>
      <div style="overflow-x:auto;">
        <table>
          <thead><tr><th>Campaign</th><th>Spend</th><th>Demos</th><th>CPD</th><th>CTR</th><th>CPM</th><th>CPC</th></tr></thead>
          <tbody>${campaignRows || '<tr><td colspan="7" style="opacity:0.5; text-align:center; padding:20px;">No campaign data</td></tr>'}</tbody>
        </table>
      </div>
    </div>
    <div class="section">
      <h2>Campaign Action Items</h2>
      ${recsHtml}
    </div>
  </div>

  <!-- CROSS-CHANNEL COMPARISON -->
  <div class="section">
    <h2>Cross-Channel CPD Comparison (30d)</h2>
    <div style="overflow-x:auto;">
      <table>
        <thead><tr><th>Channel</th><th>Spend</th><th>Demos</th><th>CPD</th><th>CTR</th><th>CPM</th></tr></thead>
        <tbody>${channelCompRows || '<tr><td colspan="6" style="opacity:0.5;">No channel data</td></tr>'}</tbody>
      </table>
    </div>
  </div>

  <!-- DISQUAL BREAKDOWN + PIPELINE -->
  <div class="two-col">
    <div class="section">
      <h2>Disqualification Breakdown (30d)</h2>
      <div style="margin-top:8px;">${dqHtml}</div>
    </div>
    <div class="section">
      <h2>Pipeline Health (30d)</h2>
      <table>
        <tbody>
          <tr><td>Demos Booked</td><td><strong>${p30.demosBooked}</strong></td></tr>
          <tr><td>Demos Happened</td><td><strong>${p30.demosHappened}</strong></td></tr>
          <tr><td>No Shows</td><td>${p30.noShow}</td></tr>
          <tr><td>Cancelled</td><td>${p30.cancelled}</td></tr>
          <tr><td>Disqualified</td><td style="color:${p30.disqualRate > DISQUAL_WARN_THRESHOLD ? '#F59E0B':'#fff'}">${p30.disqualified} (${fmtPct(p30.disqualRate)})</td></tr>
          <tr><td>Closed Won</td><td><strong>${p30.closedWon}</strong></td></tr>
          <tr><td>MRR (Closed Won)</td><td><strong>${fmt$(p30.mrr)}</strong></td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- AUDIENCE PLAYBOOK -->
  <div class="section">
    <h2>Audience & Targeting Playbook</h2>
    <div class="three-col">
      <div>
        <h3>Recommended Exclusions</h3>
        ${playbookExclHtml}
      </div>
      <div>
        <h3>Layering Strategies to Test</h3>
        ${playbookLayerHtml}
      </div>
      <div>
        <h3>Bid Strategy Recommendations</h3>
        ${playbookBidHtml}
      </div>
    </div>
  </div>

</div>
<footer>FrontrowMD · Agent 4 LinkedIn Optimizer · ${new Date().toLocaleDateString('en-US',{month:'long',year:'numeric'})}</footer>
</body>
</html>`;
}

// ─── DELIVERY ─────────────────────────────────────────────────────────────────

async function postToSlack(message) {
  if (!SLACK_WEBHOOK) { console.warn('⚠️  SLACK_WEBHOOK not set — skipping Slack.'); return; }
  try {
    const res = await fetch(SLACK_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: message }),
    });
    if (!res.ok) throw new Error(`Slack HTTP ${res.status}`);
    console.log('✅ Slack posted');
  } catch (e) { console.error('Slack error:', e.message); }
}

async function sendEmail(htmlContent, txtContent, dashboardUrl) {
  if (!EMAIL_FROM || !EMAIL_PASS || !EMAIL_TO) { console.warn('⚠️  Email not configured — skipping.'); return; }
  const transporter = nodemailer.createTransport({ service: 'gmail', auth: { user: EMAIL_FROM, pass: EMAIL_PASS } });
  const date = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  const urlLine = dashboardUrl ? `<p style="font-family:sans-serif;margin:16px 0;"><a href="${dashboardUrl}" style="color:#72A4BF;">View Full Dashboard →</a></p>` : '';
  const emailHtml = urlLine + htmlContent;
  try {
    await transporter.sendMail({
      from: EMAIL_FROM,
      to: EMAIL_TO,
      subject: `FrontrowMD LinkedIn Optimizer — ${date}`,
      html: emailHtml,
      text: txtContent + (dashboardUrl ? `\n\nView Full Dashboard: ${dashboardUrl}` : ''),
      attachments: [{ filename: `linkedin-optimizer-${date.replace(/\s/g,'-')}.html`, content: htmlContent, contentType: 'text/html' }],
    });
    console.log('✅ Email sent');
  } catch (e) { console.error('Email error:', e.message); }
}

async function deployToGitHub(htmlContent) {
  if (!GITHUB_TOKEN || !GITHUB_OWNER || !GITHUB_REPO) { console.warn('⚠️  GitHub not configured — skipping.'); return null; }
  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/index.html`;
  const pagesUrl = `https://${GITHUB_OWNER}.github.io/${GITHUB_REPO}/`;
  try {
    let sha;
    const getRes = await fetch(url, { headers: { Authorization: `token ${GITHUB_TOKEN}`, Accept: 'application/vnd.github.v3+json' } });
    if (getRes.ok) { const d = await getRes.json(); sha = d.sha; }
    const body = { message: `LinkedIn optimizer update ${toDateStr(new Date())}`, content: Buffer.from(htmlContent).toString('base64'), ...(sha ? { sha } : {}) };
    const putRes = await fetch(url, { method: 'PUT', headers: { Authorization: `token ${GITHUB_TOKEN}`, Accept: 'application/vnd.github.v3+json', 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    if (!putRes.ok) throw new Error(`GitHub PUT ${putRes.status}`);
    console.log(`✅ Dashboard deployed: ${pagesUrl}`);
    return pagesUrl;
  } catch (e) { console.error('GitHub error:', e.message); return null; }
}
}

// ─── UTILITIES ────────────────────────────────────────────────────────────────

function num(v) { return typeof v === 'number' ? v : parseFloat(v) || 0; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function fmt$(n) { return n >= 1000 ? '$' + (n / 1000).toFixed(1) + 'K' : '$' + n.toFixed(2); }
function fmtPct(n) { return (n * 100).toFixed(2) + '%'; }
function fmtNum(n) { return n.toLocaleString('en-US'); }
function padStr(s, len) { return String(s).padEnd(len); }
function escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// Summarize Windsor rows into totals
function summarizeWindsor(rows) {
  const t = { spend: 0, clicks: 0, impressions: 0, demos: 0 };
  for (const r of rows) {
    t.spend += num(r.spend);
    t.clicks += num(r.clicks);
    t.impressions += num(r.impressions);
    t.demos += num(r.conversions_hubspot_meeting_booked);
  }
  t.ctr = t.impressions > 0 ? t.clicks / t.impressions : 0;
  t.cpm = t.impressions > 0 ? (t.spend / t.impressions) * 1000 : 0;
  t.cpd = t.demos > 0 ? t.spend / t.demos : 0;
  t.cpc = t.clicks > 0 ? t.spend / t.clicks : 0;
  return t;
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('🔗 FrontrowMD LinkedIn Campaign Optimizer starting...\n');

  if (!WINDSOR_API_KEY) { console.error('❌ WINDSOR_API_KEY not set in .env'); process.exit(1); }
  if (!HUBSPOT_TOKEN) { console.error('❌ HUBSPOT_TOKEN not set in .env'); process.exit(1); }

  const windows = getWindows();
  console.log(`📅 Date windows:`);
  console.log(`   Yesterday:  ${windows.yesterday.from}`);
  console.log(`   7-Day:      ${windows.d7.from} → ${windows.d7.to}`);
  console.log(`   30-Day:     ${windows.d30.from} → ${windows.d30.to}`);
  console.log(`   Prior Mo:   ${windows.prevMonth.from} → ${windows.prevMonth.to}\n`);

  // ── FETCH DATA IN PARALLEL ──
  console.log('📡 Fetching Windsor.ai data...');
  const [liRows7, liRows30, liRowsPrev, allRows30] = await Promise.all([
    fetchLinkedInData(windows.d7.from, windows.d7.to),
    fetchLinkedInData(windows.d30.from, windows.d30.to),
    fetchLinkedInData(windows.prevMonth.from, windows.prevMonth.to),
    fetchAllChannelData(windows.d30.from, windows.d30.to),
  ]);
  console.log(`   LinkedIn rows: 7d=${liRows7.length}, 30d=${liRows30.length}, prev=${liRowsPrev.length}`);
  console.log(`   All channel rows: ${allRows30.length}`);

  // ── AGGREGATE LINKEDIN ──
  const liData = {
    d7:        summarizeWindsor(liRows7),
    d30:       summarizeWindsor(liRows30),
    prevMonth: summarizeWindsor(liRowsPrev),
  };
  const campaigns30 = aggregateByCampaign(liRows30);
  const allChannels = { d30: aggregateChannels(allRows30) };

  // Budget pacing
  const now = new Date();
  const daysInMonth = new Date(now.getUTCFullYear(), now.getUTCMonth() + 1, 0).getUTCDate();
  const dayOfMonth = now.getUTCDate();
  const expectedPace = dayOfMonth / daysInMonth;
  const budgetPaced = LI_MONTHLY_BUDGET > 0 ? liData.d30.spend / LI_MONTHLY_BUDGET : 0; // rough proxy

  console.log(`   LinkedIn 30d: Spend=${fmt$(liData.d30.spend)}, Demos=${liData.d30.demos}, CPD=${fmt$(liData.d30.cpd)}`);

  // ── FETCH HUBSPOT ──
  console.log('📡 Fetching HubSpot CRM data...');
  const wideFrom = windows.prevMonth.from;
  const wideTo = windows.d30.to;
  const rawHubSpot = await fetchAllHubSpotData(wideFrom, wideTo);
  console.log(`   Contacts: ${rawHubSpot.contacts.length}, Deals: ${rawHubSpot.deals.length}`);

  const pipelines = {
    d7:        buildPipelineMetrics(sliceWindow(rawHubSpot, windows.d7.from, windows.d7.to)),
    d30:       buildPipelineMetrics(sliceWindow(rawHubSpot, windows.d30.from, windows.d30.to)),
    prevMonth: buildPipelineMetrics(sliceWindow(rawHubSpot, windows.prevMonth.from, windows.prevMonth.to)),
  };
  console.log(`   30d pipeline: Booked=${pipelines.d30.demosBooked}, Disqual=${pipelines.d30.disqualified} (${fmtPct(pipelines.d30.disqualRate)})`);

  // ── BUILD INTELLIGENCE ──
  console.log('🧠 Running intelligence engine...');
  const intelligence = buildLinkedInIntelligence({
    liData30: liData.d30,
    allChannels30: allChannels.d30,
    pipeline30: pipelines.d30,
    pipelinePrevMonth: pipelines.prevMonth,
    budgetPaced,
  });
  console.log(`   Alerts: ${intelligence.alerts.length}, Warnings: ${intelligence.warnings.length}, Opportunities: ${intelligence.opportunities.length}, Wins: ${intelligence.wins.length}`);

  // ── CAMPAIGN RECOMMENDATIONS ──
  const campaignRecs = buildCampaignRecommendations(campaigns30, pipelines.d30);
  const audiencePlaybook = buildAudiencePlaybook(pipelines.d30);

  // ── BUILD OUTPUTS ──
  console.log('📄 Building report outputs...');
  const txtReport = buildTextReport({ windows, liData, allChannels, pipelines, intelligence, campaignRecs, audiencePlaybook, campaigns30 });
  const htmlDashboard = buildDashboard({ liData, allChannels, pipelines, intelligence, campaignRecs, audiencePlaybook, campaigns30, windows });

  // ── WRITE FILES ──
  const dateStr = toDateStr(new Date());
  const txtPath = path.join(__dirname, `linkedin-optimizer-${dateStr}.txt`);
  const htmlPath = path.join(__dirname, `linkedin-optimizer-${dateStr}.html`);
  fs.writeFileSync(txtPath, txtReport, 'utf8');
  fs.writeFileSync(htmlPath, htmlDashboard, 'utf8');
  console.log(`💾 Report saved: ${txtPath}`);
  console.log(`💾 Dashboard saved: ${htmlPath}`);

  // ── DELIVER — deploy first so URL is available for Slack/email ──
  const dashboardUrl = await deployToGitHub(htmlDashboard);
  const slackMsg = buildSlackSummary({ liData, pipelines, intelligence, windows, dashboardUrl });
  await postToSlack(slackMsg);
  await sendEmail(htmlDashboard, txtReport, dashboardUrl);

  console.log('\n✅ LinkedIn Optimizer complete!');

  // Print summary to console
  console.log('\n' + '─'.repeat(60));
  console.log('SUMMARY');
  console.log('─'.repeat(60));
  console.log(`LinkedIn CPD (30d): ${fmt$(liData.d30.cpd)} vs target ${fmt$(CPD_TARGET)}`);
  console.log(`Disqual Rate (30d): ${fmtPct(pipelines.d30.disqualRate)}`);
  console.log(`Show Rate (30d):    ${fmtPct(pipelines.d30.showRate)}`);
  console.log(`Campaigns tracked:  ${Object.keys(campaigns30).length}`);
  if (intelligence.alerts.length > 0) {
    console.log(`\n⚠️  ALERTS (${intelligence.alerts.length}):`);
    intelligence.alerts.forEach(a => console.log(`  ${a}`));
  }
}

main().catch(err => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
