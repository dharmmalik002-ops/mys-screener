import React, { Suspense, lazy, useEffect, useState, useMemo } from 'react';
import { activatable } from "../lib/activate";
import { getCompanyLiveNews, type CompanyFundamentals, type LiveNewsItem, type MarketKey, type DetailedNews } from '../lib/api';
import './PremiumResearchPanel.css';

const RatioTrendChart = lazy(() => import('./fundacharts/RatioTrendChart').then(m => ({ default: m.RatioTrendChart })));
const ShareholdingChart = lazy(() => import('./fundacharts/ShareholdingChart').then(m => ({ default: m.ShareholdingChart })));
const CashFlowChart = lazy(() => import('./fundacharts/CashFlowChart').then(m => ({ default: m.CashFlowChart })));
const FundamentalScores = lazy(() => import('./fundacharts/FundamentalScores').then(m => ({ default: m.FundamentalScores })));
const DCFCalculator = lazy(() => import('./fundacharts/DCFCalculator').then(m => ({ default: m.DCFCalculator })));
const QuarterlyChart = lazy(() => import('./fundacharts/QuarterlyChart').then(m => ({ default: m.QuarterlyChart })));
const AnnualPLChart = lazy(() => import('./fundacharts/AnnualPLChart').then(m => ({ default: m.AnnualPLChart })));

const ChartTabFallback = () => (
  <div style={{ textAlign: 'center', padding: '40px 0', color: 'rgba(255,255,255,0.4)', fontSize: '0.85rem' }}>
    Loading charts…
  </div>
);

interface PremiumResearchPanelProps {
  symbol: string;
  market: MarketKey;
  fundamentals: CompanyFundamentals;
}

type TabKey = 'overview' | 'results' | 'fundamentals' | 'guidance' | 'news' | 'risks' | 'charts' | 'valuation';

export const PremiumResearchPanel: React.FC<PremiumResearchPanelProps> = ({
  symbol,
  market,
  fundamentals,
}) => {
  const [activeTab, setActiveTab] = useState<TabKey>('overview');
  const [liveNews, setLiveNews] = useState<LiveNewsItem[]>([]);

  useEffect(() => {
    setLiveNews([]);
    let cancelled = false;
    getCompanyLiveNews(symbol, market, 15)
      .then((res) => { if (!cancelled) setLiveNews(res.items); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [symbol, market]);

  const tabs = [
    { key: 'overview', label: 'Growth Triggers' },
    { key: 'news', label: 'News & Updates' },
    { key: 'guidance', label: 'Active Guidance' },
    { key: 'results', label: 'Latest Results' },
    { key: 'fundamentals', label: 'Fundamentals' },
    { key: 'charts', label: 'Charts' },
    { key: 'valuation', label: 'Valuation' },
    { key: 'risks', label: 'Growth Risks' },
  ];

  const formatCurrency = (val: number | null | undefined, precision = 2) => {
    if (val === null || val === undefined) return 'N/A';
    return `₹${val.toLocaleString("en-IN", { minimumFractionDigits: precision, maximumFractionDigits: precision })}Cr`;
  };

  const formatPercent = (val: number | null | undefined) => {
    if (val === null || val === undefined) return 'N/A';
    return `${val > 0 ? '+' : ''}${val.toFixed(2)}%`;
  };

  const editorialNews = useMemo(() => 
    fundamentals.detailed_news.filter(n => n.is_editorial || n.source_type === 'Editorial News'),
    [fundamentals.detailed_news]
  );

  const officialNews = useMemo(() => 
    fundamentals.detailed_news.filter(n => !n.is_editorial && n.source_type !== 'Editorial News'),
    [fundamentals.detailed_news]
  );

  const renderOverview = () => (
    <div className="fade-in">
      <section className="research-card growth-triggers-hero">
        <div className="research-card-head">
          <div className="section-title-wrap">
            <h3 className="section-title-gold">🚀 Future Growth Triggers</h3>
            <p className="text-secondary small">High-conviction drivers for business expansion and stock rerating</p>
          </div>
        </div>

        <div className="triggers-grid">
          {fundamentals.future_growth_triggers?.length > 0 ? fundamentals.future_growth_triggers.map((t, i) => (
            <div key={i} className={`trigger-tile horizon-${t.horizon}`}>
              <div className="trigger-head">
                <span className="trigger-category">{t.category?.replace(/_/g, ' ')}</span>
                <span className="trigger-date">{t.source_date || 'Recent'}</span>
              </div>
              <h4 className="trigger-title">{t.title}</h4>
              <p className="trigger-impact">{t.why_it_matters}</p>
              <div className="trigger-footer">
                <span className={`impact-badge impact-${t.impact_area}`}>{t.impact_area?.toUpperCase()}</span>
                <span className="trigger-horizon">{t.horizon?.toUpperCase()}</span>
                {t.is_new && <span className="new-tag">NEW</span>}
              </div>
            </div>
          )) : (
            <div className="empty-state-card">
              No specific growth triggers identified in recent management commentary.
            </div>
          )}
        </div>
      </section>

      <section className="research-card">
        <div className="research-card-head">
          <div>
            <h3>Company Snapshot</h3>
            <p className="text-secondary">{fundamentals.sector} • {fundamentals.sub_sector}</p>
          </div>
          <div className={`sentiment-pill sentiment-${fundamentals.ai_news_summary?.sentiment ?? 'neutral'}`}>
            {fundamentals.ai_news_summary?.sentiment?.toUpperCase() ?? 'NEUTRAL'} SENTIMENT
          </div>
        </div>
        <p className="business-description">{fundamentals.about || fundamentals.business_summary}</p>
      </section>

      {fundamentals.ai_news_summary && (
        <section className="research-card">
          <h4>AI Strategic Narrative</h4>
          <p style={{ margin: '12px 0', fontSize: '14px', lineHeight: '1.6' }}>{fundamentals.ai_news_summary.summary}</p>
          <ul className="news-detailed-points grid-2">
            {fundamentals.ai_news_summary.key_points.map((p, i) => <li key={i}>{p}</li>)}
          </ul>
        </section>
      )}
    </div>
  );

  const renderResults = () => {
    const formatChange = (val: number | null | undefined) => {
      if (val === null || val === undefined) return null;
      const cls = val >= 0 ? 'text-uptrend' : 'text-downtrend';
      return <span className={cls}>{val >= 0 ? '+' : ''}{val.toFixed(1)}%</span>;
    };

    return (
    <div className="fade-in">
      {fundamentals.growth && (
        <section className="research-card results-headline-strip">
          <div className="results-kpi-row">
            <div className="kpi-block">
              <span className="kpi-label">Revenue YoY</span>
              <span className={`kpi-value ${(fundamentals.growth.sales_yoy_pct ?? 0) >= 0 ? 'kpi-positive' : 'kpi-negative'}`}>
                {formatPercent(fundamentals.growth.sales_yoy_pct)}
              </span>
            </div>
            <div className="kpi-block">
              <span className="kpi-label">Revenue QoQ</span>
              <span className={`kpi-value ${(fundamentals.growth.sales_qoq_pct ?? 0) >= 0 ? 'kpi-positive' : 'kpi-negative'}`}>
                {formatPercent(fundamentals.growth.sales_qoq_pct)}
              </span>
            </div>
            <div className="kpi-block">
              <span className="kpi-label">Net Profit YoY</span>
              <span className={`kpi-value ${(fundamentals.growth.profit_yoy_pct ?? 0) >= 0 ? 'kpi-positive' : 'kpi-negative'}`}>
                {formatPercent(fundamentals.growth.profit_yoy_pct)}
              </span>
            </div>
            <div className="kpi-block">
              <span className="kpi-label">Net Profit QoQ</span>
              <span className={`kpi-value ${(fundamentals.growth.profit_qoq_pct ?? 0) >= 0 ? 'kpi-positive' : 'kpi-negative'}`}>
                {formatPercent(fundamentals.growth.profit_qoq_pct)}
              </span>
            </div>
            <div className="kpi-block">
              <span className="kpi-label">OPM Latest</span>
              <span className="kpi-value kpi-neutral">{fundamentals.growth.operating_margin_latest_pct?.toFixed(1) ?? 'N/A'}%</span>
            </div>
            <div className="kpi-block">
              <span className="kpi-label">Net Margin</span>
              <span className="kpi-value kpi-neutral">{fundamentals.growth.net_margin_latest_pct?.toFixed(1) ?? 'N/A'}%</span>
            </div>
          </div>
        </section>
      )}

      {fundamentals.results_summary && (
        <section className="research-card">
          <div className="research-card-head">
            <h3>Latest Quarter Performance</h3>
            <div className={`badge badge-impact ${fundamentals.results_summary.beat_miss?.toLowerCase().includes('beat') ? 'sentiment-positive' : fundamentals.results_summary.beat_miss?.toLowerCase().includes('miss') ? 'sentiment-negative' : 'sentiment-neutral'}`}>
              {fundamentals.results_summary.beat_miss}
            </div>
          </div>

          <div className="results-analysis-blocks">
            <div className="analysis-block">
              <h4>Segment Highlights</h4>
              <p>{fundamentals.results_summary.segment_performance}</p>
            </div>
            {fundamentals.results_summary.margins_analysis && (
              <div className="analysis-block">
                <h4>Margin Analysis</h4>
                <p>{fundamentals.results_summary.margins_analysis}</p>
              </div>
            )}
          </div>
          <ul className="news-detailed-points grid-2">
            {fundamentals.results_summary.highlights?.map((h: string, i: number) => <li key={i}>{h}</li>)}
          </ul>
        </section>
      )}

      {fundamentals.quarterly_results.length > 0 && (
        <section className="research-card">
          <Suspense fallback={<ChartTabFallback />}>
            <QuarterlyChart data={fundamentals.quarterly_results} market={market} />
          </Suspense>
        </section>
      )}

      <section className="research-card">
        <div className="research-card-head">
          <h3>Quarterly Trajectory</h3>
          <div className="text-secondary small">Values in {market === 'india' ? '₹ Crores' : '$ Millions'}</div>
        </div>
        <div className="fundamentals-table-wrap">
          <table className="fundamentals-table">
            <thead>
              <tr>
                <th>Quarter</th>
                <th>Revenue</th>
                <th>YoY</th>
                <th>QoQ</th>
                <th>EBITDA %</th>
                <th>Net Profit</th>
                <th>Net Margin</th>
                <th>EPS</th>
                <th>Beat/Miss</th>
              </tr>
            </thead>
            <tbody>
              {[...fundamentals.quarterly_results].reverse().map(q => {
                const netMargin = q.net_profit_crore && q.sales_crore && q.sales_crore > 0
                  ? ((q.net_profit_crore / q.sales_crore) * 100)
                  : null;
                return (
                  <tr key={q.period}>
                    <td style={{ fontWeight: 600 }}>{q.period}</td>
                    <td>{formatCurrency(q.sales_crore)}</td>
                    <td>{formatChange(q.yoy_change_pct) ?? <span className="text-secondary">—</span>}</td>
                    <td>{formatChange(q.qoq_change_pct) ?? <span className="text-secondary">—</span>}</td>
                    <td className={q.ebitda_margin_pct && q.ebitda_margin_pct >= 20 ? 'text-uptrend' : ''}>
                      {q.ebitda_margin_pct != null ? `${q.ebitda_margin_pct.toFixed(1)}%` : <span className="text-secondary">—</span>}
                    </td>
                    <td>{formatCurrency(q.net_profit_crore)}</td>
                    <td>{netMargin != null ? `${netMargin.toFixed(1)}%` : <span className="text-secondary">—</span>}</td>
                    <td style={{ fontWeight: 600 }}>{q.eps ?? '—'}</td>
                    <td>
                      {q.beat_miss ? (
                        <span className={`badge-slim badge-${q.beat_miss}`}>{q.beat_miss}</span>
                      ) : <span className="text-secondary">—</span>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      {fundamentals.latest_earnings_key_metrics && Object.keys(fundamentals.latest_earnings_key_metrics).length > 0 && (
        <section className="research-card">
          <h3>Latest Earnings Key Metrics</h3>
          <div className="kpi-row-compact">
            {Object.entries(fundamentals.latest_earnings_key_metrics).map(([k, v]) => (
              <div key={k} className="kpi-compact-block">
                <span className="kpi-compact-label">{k}</span>
                <span className="kpi-compact-value">{String(v)}</span>
              </div>
            ))}
          </div>
        </section>
      )}
    </div>
  );
  };

  const renderNews = () => {
    // Hard rule: Latest News = editorial only. official_updates = releases/filings only.
    // Never mix them. Never promote company releases to Latest News.
    const editorial = (fundamentals.latest_editorial_news && fundamentals.latest_editorial_news.length > 0)
      ? fundamentals.latest_editorial_news
      : editorialNews;
    const official = (fundamentals.official_updates && fundamentals.official_updates.length > 0)
      ? fundamentals.official_updates
      : officialNews;

    return (
      <div className="fade-in">
        {/* ── Live RSS news ──────────────────────────────────────────── */}
        {liveNews.length > 0 && (
          <div style={{ marginBottom: 20 }}>
            <div className="news-section-title" style={{ marginBottom: 10 }}>
              <span style={{ color: '#ef4444', fontSize: '0.7rem' }}>●</span> Live News
              <span className="news-section-badge" style={{ background: 'rgba(239,68,68,0.15)', color: '#ef4444', border: '1px solid rgba(239,68,68,0.3)' }}>RSS Feeds</span>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {liveNews.map((item) => (
                <div
                  key={item.id}
                  className="news-article-card editorial-card"
                  style={{ borderLeft: `3px solid ${item.source.color}`, cursor: 'default' }}
                >
                  <div className="news-article-meta">
                    <span
                      className="badge badge-editorial"
                      style={{
                        background: `${item.source.color}22`,
                        color: item.source.color,
                        border: `1px solid ${item.source.color}44`,
                      }}
                    >
                      {item.source.name}
                    </span>
                    <span className="text-secondary" style={{ fontSize: '0.7rem' }}>{item.category}</span>
                    {item.pub_date && (
                      <span className="news-date">
                        {(() => {
                          const diff = Date.now() - new Date(item.pub_date).getTime();
                          if (isNaN(diff)) return '';
                          const mins = Math.floor(diff / 60000);
                          if (mins < 60) return `${mins}m ago`;
                          const hrs = Math.floor(mins / 60);
                          if (hrs < 24) return `${hrs}h ago`;
                          return `${Math.floor(hrs / 24)}d ago`;
                        })()}
                      </span>
                    )}
                  </div>
                  <h4>{item.title}</h4>
                  {item.description && <p className="news-summary">{item.description}</p>}
                  {item.link && (
                    <a href={item.link} target="_blank" rel="noopener noreferrer" className="news-read-link">
                      Read →
                    </a>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="news-feed-split">
          <div className="news-column">
            <div className="news-section-title">
              Latest Editorial News
              <span className="news-section-badge">Journalism Only</span>
            </div>
            {editorial.length > 0 ? (
              editorial
                .slice()
                .sort((a, b) => (b.relevance_score ?? 0) - (a.relevance_score ?? 0))
                .map((news, i) => (
                <div key={i} className="news-article-card editorial-card">
                  <div className="news-article-meta">
                    <span className="badge badge-editorial">{news.source}</span>
                    {news.impact_area && <span className="impact-indicator">{news.impact_area}</span>}
                    {news.published_date && <span className="news-date">{news.published_date}</span>}
                    <span className="relevance-score">{Math.round((news.relevance_score ?? 0) * 100)}% rel.</span>
                  </div>
                  <h4>{news.title}</h4>
                  {news.url ? (
                    <a href={news.url} target="_blank" rel="noopener noreferrer" className="news-read-link">Read Article →</a>
                  ) : null}
                  <p className="news-summary">{news.summary}</p>
                  {news.why_it_matters && (
                    <div className="news-why-it-matters">
                      <strong>Value Trigger:</strong> {news.why_it_matters}
                    </div>
                  )}
                  {news.impact_tags && news.impact_tags.length > 0 && (
                    <div className="impact-tags">
                      {news.impact_tags.map(tag => <span key={tag} className="tag-pill">{tag}</span>)}
                    </div>
                  )}
                </div>
              ))
            ) : (
              <div className="empty-state editorial-fallback">
                <p>No fresh editorial coverage found from verified news outlets.</p>
                <p className="text-secondary small">Showing latest official company updates separately in the right column.</p>
              </div>
            )}
          </div>

          <div className="news-column">
            <div className="news-section-title">
              <span aria-hidden>🔔</span> Official Updates
              <span className="news-section-badge">Filings &amp; Releases</span>
            </div>
            {official.length > 0 ? official.map((news, i) => (
              <div key={i} className="news-article-card official-card">
                <div className="news-article-meta">
                  <span className="badge badge-official">
                    {news.classification === 'exchange_filing' ? 'Exchange Filing'
                      : news.classification === 'transcript' ? 'Transcript'
                      : news.classification === 'investor_presentation' ? 'Presentation'
                      : 'Company Release'}
                  </span>
                  <span className="text-secondary">{news.source}</span>
                  {news.published_date && <span className="news-date">{news.published_date}</span>}
                </div>
                <h4>{news.title}</h4>
                {news.summary && <p className="news-summary">{news.summary}</p>}
                {news.url && (
                  <div className="news-footer">
                    <a href={news.url} target="_blank" rel="noopener noreferrer" className="view-source">View Source Document →</a>
                  </div>
                )}
              </div>
            )) : <div className="empty-state">No official filings detected recently.</div>}
          </div>
        </div>
      </div>
    );
  };

  const renderGuidance = () => {
    // Sort: valid guidance first, stale last
    const sorted = [...(fundamentals.management_guidance ?? [])].sort((a, b) => {
      if (a.is_stale === b.is_stale) return 0;
      return a.is_stale ? 1 : -1;
    });
    const activeGuidance = sorted.filter(g => !g.is_stale);
    const staleGuidance = sorted.filter(g => g.is_stale);

    return (
    <div className="fade-in">
      <section className="research-card">
        <div className="research-card-head">
          <h3>Active Management Guidance</h3>
          <div className="guidance-date-context text-secondary small">As of April 2026</div>
        </div>

        {activeGuidance.length === 0 && (
          <div className="empty-state">
            No current active guidance provided by management.
            {staleGuidance.length > 0 && (
              <span className="text-secondary small"> Stale historical guidance shown below for reference.</span>
            )}
          </div>
        )}

        <div className="management-guidance-list">
          {activeGuidance.map((g, i) => (
            <div key={i} className="guidance-item guidance-valid">
              <div className="research-card-head">
                <div className="guidance-banner-wrap">
                  <h4 className="guidance-period-label">{g.fiscal_period || g.fiscal_year} Guidance</h4>
                  <div className="validity-banner banner-valid">ACTIVE</div>
                </div>
                <div className="guidance-meta text-secondary">
                  {g.guidance_source} · {g.source_date || g.guidance_date || 'Recent'}
                </div>
              </div>
              <div className="guidance-metrics-row">
                <div className="g-metric">
                  <span className="g-label">Revenue Growth</span>
                  <span className="g-value">{g.revenue_growth_guidance_pct != null ? formatPercent(g.revenue_growth_guidance_pct) : 'Qualitative'}</span>
                </div>
                <div className="g-metric">
                  <span className="g-label">EBITDA</span>
                  <span className="g-value">{g.ebitda_guidance_pct != null ? formatPercent(g.ebitda_guidance_pct) : '—'}</span>
                </div>
                <div className="g-metric">
                  <span className="g-label">Capex</span>
                  <span className="g-value">{g.capex_guidance_crore != null ? formatCurrency(g.capex_guidance_crore) : '—'}</span>
                </div>
                <div className="g-metric">
                  <span className="g-label">Type</span>
                  <span className="g-value">{g.guidance_type ?? '—'}</span>
                </div>
              </div>
              <div className="guidance-points-wrap">
                <strong>Key Management Directives:</strong>
                <ul className="news-detailed-points grid-2">
                  {g.key_guidance_points.map((p, j) => <li key={j}>{p}</li>)}
                </ul>
              </div>
            </div>
          ))}
        </div>

        {staleGuidance.length > 0 && (
          <details className="stale-guidance-section">
            <summary className="stale-guidance-toggle">
              Historical / Stale Guidance ({staleGuidance.length} items — periods already concluded)
            </summary>
            <div className="management-guidance-list" style={{ marginTop: 12 }}>
              {staleGuidance.map((g, i) => (
                <div key={i} className="guidance-item guidance-stale">
                  <div className="research-card-head">
                    <div className="guidance-banner-wrap">
                      <h4 className="guidance-period-label">{g.fiscal_period || g.fiscal_year} Guidance</h4>
                      <div className="validity-banner banner-stale">STALE</div>
                    </div>
                    <div className="guidance-meta text-secondary">
                      {g.guidance_source} · {g.source_date || g.guidance_date || 'N/A'}
                    </div>
                  </div>
                  <div className="stale-warning-box">
                    ⚠️ This guidance refers to a concluded fiscal period ({g.fiscal_period || g.fiscal_year}).
                    It is no longer active and shown here for historical reference only.
                  </div>
                  <div className="guidance-points-wrap">
                    <ul className="news-detailed-points grid-2">
                      {g.key_guidance_points.map((p, j) => <li key={j}>{p}</li>)}
                    </ul>
                  </div>
                </div>
              ))}
            </div>
          </details>
        )}
      </section>

      {fundamentals.guidance_tracker && fundamentals.guidance_tracker.length > 0 && (
        <section className="research-card">
          <h3>Guidance Revision Tracker</h3>
          <div className="guidance-list">
            <div className="guidance-tracker-row guidance-header">
              <div>Date</div>
              <div>Previous Outlook</div>
              <div>Revised Forecast</div>
              <div>Reason</div>
            </div>
            {fundamentals.guidance_tracker.map((g, i) => (
              <div key={i} className="guidance-tracker-row">
                <div className="text-secondary">{g.date}</div>
                <div className="text-bold">{g.previous}</div>
                <div className="text-accent">{g.current}</div>
                <div className="badge-slim">{g.reason}</div>
              </div>
            ))}
          </div>
        </section>
      )}
    </div>
  );
  };

  const renderContent = () => {
    switch (activeTab) {
      case 'overview': return renderOverview();
      case 'results': return renderResults();
      case 'news': return renderNews();
      case 'guidance': return renderGuidance();
      case 'fundamentals': {
        const latestCF = fundamentals.cash_flow?.[0];
        const latestBS = fundamentals.balance_sheet?.[0];
        const latestSH = fundamentals.shareholding_pattern?.[0];
        return (
          <div className="fade-in">
            {/* Valuation Snapshot */}
            {fundamentals.valuation && (
              <section className="research-card">
                <h3>Valuation Snapshot</h3>
                <div className="kpi-row-compact">
                  {[
                    { label: 'P/E', value: fundamentals.valuation.pe_ratio?.toFixed(1) },
                    { label: 'PEG', value: fundamentals.valuation.peg_ratio?.toFixed(2) },
                    { label: 'Mkt Cap', value: fundamentals.valuation.market_cap_crore ? formatCurrency(fundamentals.valuation.market_cap_crore) : null },
                    { label: 'ROE %', value: fundamentals.valuation.roe_pct != null ? `${fundamentals.valuation.roe_pct.toFixed(1)}%` : null },
                    { label: 'ROCE %', value: fundamentals.valuation.roce_pct != null ? `${fundamentals.valuation.roce_pct.toFixed(1)}%` : null },
                    { label: 'Div Yield', value: fundamentals.valuation.dividend_yield_pct != null ? `${fundamentals.valuation.dividend_yield_pct.toFixed(2)}%` : null },
                    { label: 'OPM %', value: fundamentals.valuation.operating_margin_pct != null ? `${fundamentals.valuation.operating_margin_pct.toFixed(1)}%` : null },
                    { label: 'Net Margin', value: fundamentals.valuation.net_margin_pct != null ? `${fundamentals.valuation.net_margin_pct.toFixed(1)}%` : null },
                  ].map(({ label, value }) => value ? (
                    <div key={label} className="kpi-compact-block">
                      <span className="kpi-compact-label">{label}</span>
                      <span className="kpi-compact-value">{value}</span>
                    </div>
                  ) : null)}
                </div>
              </section>
            )}

            {/* Key Ratios History */}
            {fundamentals.financial_ratios.length > 0 && (
              <section className="research-card">
                <h3>Key Ratios History</h3>
                <div className="fundamentals-table-wrap">
                  <table className="fundamentals-table">
                    <thead>
                      <tr>
                        <th>Period</th>
                        <th>ROE %</th>
                        <th>ROCE %</th>
                        <th>D/E Ratio</th>
                        <th>Current Ratio</th>
                        <th>Int. Coverage</th>
                        <th>Asset Turnover</th>
                      </tr>
                    </thead>
                    <tbody>
                      {fundamentals.financial_ratios.map(r => (
                        <tr key={r.period}>
                          <td style={{ fontWeight: 600 }}>{r.period}</td>
                          <td className={r.roe_pct && r.roe_pct >= 15 ? 'text-uptrend' : ''}>{formatPercent(r.roe_pct)}</td>
                          <td className={r.roce_pct && r.roce_pct >= 15 ? 'text-uptrend' : ''}>{formatPercent(r.roce_pct)}</td>
                          <td className={r.debt_to_equity_ratio && r.debt_to_equity_ratio > 1 ? 'text-downtrend' : ''}>{r.debt_to_equity_ratio?.toFixed(2) ?? '—'}</td>
                          <td>{r.current_ratio?.toFixed(2) ?? '—'}</td>
                          <td>{r.interest_coverage?.toFixed(1) ?? '—'}</td>
                          <td>{r.asset_turnover?.toFixed(2) ?? '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
            )}

            {/* Cash Flow (most recent year) */}
            {latestCF && (
              <section className="research-card">
                <h3>Cash Flow — {latestCF.period}</h3>
                <div className="kpi-row-compact">
                  {[
                    { label: 'Operating CFO', value: latestCF.operating_cash_flow_crore != null ? formatCurrency(latestCF.operating_cash_flow_crore) : null },
                    { label: 'Capex', value: latestCF.capital_expenditure_crore != null ? formatCurrency(latestCF.capital_expenditure_crore) : null },
                    { label: 'Free Cash Flow', value: latestCF.free_cash_flow_crore != null ? formatCurrency(latestCF.free_cash_flow_crore) : null },
                    { label: 'Investing', value: latestCF.investing_cash_flow_crore != null ? formatCurrency(latestCF.investing_cash_flow_crore) : null },
                    { label: 'Financing', value: latestCF.financing_cash_flow_crore != null ? formatCurrency(latestCF.financing_cash_flow_crore) : null },
                  ].map(({ label, value }) => value ? (
                    <div key={label} className="kpi-compact-block">
                      <span className="kpi-compact-label">{label}</span>
                      <span className="kpi-compact-value">{value}</span>
                    </div>
                  ) : null)}
                </div>
              </section>
            )}

            {/* Shareholding Pattern */}
            {latestSH && (
              <section className="research-card">
                <h3>Shareholding Pattern — {latestSH.period}</h3>
                <div className="shareholding-grid">
                  {[
                    { label: 'Promoter', pct: latestSH.promoter_pct },
                    { label: 'FII / FPI', pct: latestSH.fii_pct },
                    { label: 'DII / Mutual Fund', pct: latestSH.dii_pct },
                    { label: 'Public', pct: latestSH.public_pct },
                  ].map(({ label, pct }) => pct != null ? (
                    <div key={label} className="shareholding-row">
                      <span className="sh-label">{label}</span>
                      <div className="sh-bar-wrap">
                        <div className="sh-bar" style={{ width: `${Math.min(pct, 100)}%` }} />
                      </div>
                      <span className="sh-pct">{pct.toFixed(1)}%</span>
                    </div>
                  ) : null)}
                </div>
              </section>
            )}

            {/* Balance Sheet (most recent year) */}
            {latestBS && (
              <section className="research-card">
                <h3>Balance Sheet — {latestBS.period}</h3>
                <div className="kpi-row-compact">
                  {[
                    { label: 'Total Assets', value: latestBS.total_assets_crore != null ? formatCurrency(latestBS.total_assets_crore) : null },
                    { label: 'Total Debt', value: latestBS.debt_crore != null ? formatCurrency(latestBS.debt_crore) : null },
                    { label: "Equity", value: latestBS.shareholders_equity_crore != null ? formatCurrency(latestBS.shareholders_equity_crore) : null },
                    { label: 'Cash & Equiv.', value: latestBS.cash_and_equivalents_crore != null ? formatCurrency(latestBS.cash_and_equivalents_crore) : null },
                    { label: 'Inventory', value: latestBS.inventory_crore != null ? formatCurrency(latestBS.inventory_crore) : null },
                    { label: 'Receivables', value: latestBS.receivables_crore != null ? formatCurrency(latestBS.receivables_crore) : null },
                  ].map(({ label, value }) => value ? (
                    <div key={label} className="kpi-compact-block">
                      <span className="kpi-compact-label">{label}</span>
                      <span className="kpi-compact-value">{value}</span>
                    </div>
                  ) : null)}
                </div>
              </section>
            )}
          </div>
        );
      }
      case 'charts':
        return (
          <Suspense fallback={<ChartTabFallback />}>
          <div className="fade-in" style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
            <section className="research-card">
              <RatioTrendChart
                ratios={fundamentals.financial_ratios ?? []}
                profitLoss={fundamentals.profit_loss ?? []}
                market={market}
              />
            </section>
            <section className="research-card">
              <ShareholdingChart data={fundamentals.shareholding_pattern ?? []} />
            </section>
            <section className="research-card">
              <CashFlowChart cashFlow={fundamentals.cash_flow ?? []} />
            </section>
            {fundamentals.profit_loss?.length > 0 && (
              <section className="research-card">
                <AnnualPLChart data={fundamentals.profit_loss} market={market} />
              </section>
            )}
          </div>
          </Suspense>
        );
      case 'valuation':
        return (
          <Suspense fallback={<ChartTabFallback />}>
          <div className="fade-in" style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
            <section className="research-card">
              <FundamentalScores
                balanceSheet={fundamentals.balance_sheet ?? []}
                cashFlow={fundamentals.cash_flow ?? []}
                profitLoss={fundamentals.profit_loss ?? []}
                ratios={fundamentals.financial_ratios ?? []}
                valuation={fundamentals.valuation ?? null}
                market={market}
              />
            </section>
            <section className="research-card">
              <DCFCalculator fundamentals={fundamentals} market={market} />
            </section>
          </div>
          </Suspense>
        );
      case 'risks':
        return (
          <div className="fade-in">
            <div className="news-feed-split">
               <div className="column">
                  <div className="news-section-title">🛡️ Strategic Growth Risks</div>
                  {fundamentals.growth_risks?.length > 0 ? fundamentals.growth_risks.map((r, i) => (
                    <div key={i} className="news-article-card risk-card" style={{ borderLeft: `4px solid var(--risk-${r.severity})` }}>
                       <div className="news-article-meta">
                          <span className={`badge risk-badge-${r.severity}`}>{r.risk_category}</span>
                          <span className="risk-severity-label">{r.severity.toUpperCase()} RISK</span>
                       </div>
                       <h4>{r.description}</h4>
                       {r.mitigation_strategy && (
                         <div className="mitigation-box">
                           <strong>Mitigation:</strong> {r.mitigation_strategy}
                         </div>
                       )}
                    </div>
                  )) : (
                    <div className="empty-state">No major growth risks identified.</div>
                  )}
               </div>
               <div className="column">
                  <div className="news-section-title">⚖️ Legacy Competitive Position</div>
                  <section className="research-card" style={{ padding: 16 }}>
                    <div>
                      <h4 className="text-accent">{fundamentals.competitive_position?.market_position || 'Market Participant'}</h4>
                      <div className="meta-small text-secondary">Est. Market Share: {fundamentals.competitive_position?.market_share_estimate || 'N/A'}</div>
                    </div>
                    <div className="advantages-list" style={{ marginTop: 12 }}>
                      <p className="small text-bold">Moat / Competitive Advantages:</p>
                      <ul className="news-detailed-points">
                        {fundamentals.competitive_position?.competitive_advantages.map((a, i) => <li key={i}>{a}</li>)}
                      </ul>
                    </div>
                  </section>
               </div>
            </div>
          </div>
        );
      default: return null;
    }
  };

  return (
    <div className="premium-research-container">
      <nav className="research-navbar">
        {tabs.map(tab => (
          <div
            key={tab.key}
            className={`nav-item ${activeTab === tab.key ? 'active' : ''}`}
            aria-current={activeTab === tab.key ? "page" : undefined}
            {...activatable(() => setActiveTab(tab.key as TabKey))}
          >
            {tab.label}
          </div>
        ))}
      </nav>
      <div className="research-content">
        {renderContent()}
      </div>
    </div>
  );
};
