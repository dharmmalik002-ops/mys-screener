import React, { useState, useMemo } from 'react';
import { type CompanyFundamentals, type MarketKey, type DetailedNews } from '../lib/api';
import './PremiumResearchPanel.css';

interface PremiumResearchPanelProps {
  symbol: string;
  market: MarketKey;
  fundamentals: CompanyFundamentals;
}

type TabKey = 'overview' | 'results' | 'fundamentals' | 'guidance' | 'news' | 'risks';

export const PremiumResearchPanel: React.FC<PremiumResearchPanelProps> = ({
  symbol,
  market,
  fundamentals,
}) => {
  const [activeTab, setActiveTab] = useState<TabKey>('overview');

  const tabs = [
    { key: 'overview', label: 'Growth Triggers' },
    { key: 'news', label: 'News & Updates' },
    { key: 'guidance', label: 'Active Guidance' },
    { key: 'results', label: 'Latest Results' },
    { key: 'fundamentals', label: 'Fundamentals' },
    { key: 'risks', label: 'Growth Risks' },
  ];

  const formatCurrency = (val: number | null | undefined, precision = 2) => {
    if (val === null || val === undefined) return 'N/A';
    const label = market === 'us' ? '$' : '₹';
    const suffix = market === 'us' ? 'M' : 'Cr';
    return `${label}${val.toLocaleString(undefined, { minimumFractionDigits: precision, maximumFractionDigits: precision })}${suffix}`;
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
              {fundamentals.quarterly_results.map(q => {
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
        <div className="news-feed-split">
          <div className="news-column">
            <div className="news-section-title">
              <span aria-hidden>📰</span> Latest Editorial News
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
      case 'fundamentals':
        return (
          <div className="fade-in">
            {/* Valuation Snapshot */}
            <section className="research-card">
              <h3>Valuation Snapshot</h3>
              <div className="kpi-row-compact">
                {[
                  { label: 'P/E', value: fundamentals.valuation?.pe_ratio?.toFixed(1) },
                  { label: 'P/B', value: fundamentals.valuation?.pb_ratio?.toFixed(2) },
                  { label: 'EV/EBITDA', value: fundamentals.valuation?.ev_ebitda?.toFixed(1) },
                  { label: 'Mkt Cap', value: fundamentals.valuation?.market_cap ? formatCurrency(fundamentals.valuation.market_cap) : null },
                  { label: 'Div Yield', value: fundamentals.valuation?.dividend_yield_pct != null ? `${fundamentals.valuation.dividend_yield_pct.toFixed(2)}%` : null },
                  { label: 'EV/Revenue', value: fundamentals.valuation?.ev_revenue?.toFixed(2) },
                ].map(({ label, value }) => value ? (
                  <div key={label} className="kpi-compact-block">
                    <span className="kpi-compact-label">{label}</span>
                    <span className="kpi-compact-value">{value}</span>
                  </div>
                ) : null)}
              </div>
            </section>

            {/* Key Ratios History */}
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
                        <td>{r.interest_coverage_ratio?.toFixed(1) ?? '—'}</td>
                        <td>{r.asset_turnover_ratio?.toFixed(2) ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            {/* Cash Flow Summary */}
            {fundamentals.cash_flow_summary && (
              <section className="research-card">
                <h3>Cash Flow Summary</h3>
                <div className="kpi-row-compact">
                  {[
                    { label: 'Operating CFO', value: fundamentals.cash_flow_summary.operating_cfo ? formatCurrency(fundamentals.cash_flow_summary.operating_cfo) : null },
                    { label: 'Capex', value: fundamentals.cash_flow_summary.capex ? formatCurrency(fundamentals.cash_flow_summary.capex) : null },
                    { label: 'Free Cash Flow', value: fundamentals.cash_flow_summary.free_cash_flow ? formatCurrency(fundamentals.cash_flow_summary.free_cash_flow) : null },
                    { label: 'FCF Yield', value: fundamentals.cash_flow_summary.fcf_yield_pct != null ? `${fundamentals.cash_flow_summary.fcf_yield_pct.toFixed(2)}%` : null },
                    { label: 'Cash Conversion', value: fundamentals.cash_flow_summary.cash_conversion_cycle ? `${fundamentals.cash_flow_summary.cash_conversion_cycle.toFixed(0)}d` : null },
                  ].map(({ label, value }) => value ? (
                    <div key={label} className="kpi-compact-block">
                      <span className="kpi-compact-label">{label}</span>
                      <span className="kpi-compact-value">{value}</span>
                    </div>
                  ) : null)}
                </div>
              </section>
            )}

            {/* Shareholding */}
            {fundamentals.shareholding_pattern && (
              <section className="research-card">
                <h3>Shareholding Pattern</h3>
                <div className="shareholding-grid">
                  {[
                    { label: 'Promoter', pct: fundamentals.shareholding_pattern.promoter_pct },
                    { label: 'FII / FPI', pct: fundamentals.shareholding_pattern.fii_pct },
                    { label: 'DII / Mutual Fund', pct: fundamentals.shareholding_pattern.dii_pct },
                    { label: 'Public', pct: fundamentals.shareholding_pattern.public_pct },
                  ].map(({ label, pct }) => pct != null ? (
                    <div key={label} className="shareholding-row">
                      <span className="sh-label">{label}</span>
                      <div className="sh-bar-wrap">
                        <div className="sh-bar" style={{ width: `${pct}%` }} />
                      </div>
                      <span className="sh-pct">{pct.toFixed(1)}%</span>
                    </div>
                  ) : null)}
                </div>
                {fundamentals.shareholding_pattern.promoter_pledge_pct != null && (
                  <div className={`pledge-warning ${fundamentals.shareholding_pattern.promoter_pledge_pct > 10 ? 'pledge-high' : ''}`}>
                    Promoter Pledge: {fundamentals.shareholding_pattern.promoter_pledge_pct.toFixed(1)}%
                    {fundamentals.shareholding_pattern.promoter_pledge_pct > 10 ? ' ⚠️ Elevated pledge' : ''}
                  </div>
                )}
              </section>
            )}

            {/* Balance Sheet Snapshot */}
            {fundamentals.balance_sheet_summary && (
              <section className="research-card">
                <h3>Balance Sheet Snapshot</h3>
                <div className="kpi-row-compact">
                  {[
                    { label: 'Total Assets', value: formatCurrency(fundamentals.balance_sheet_summary.total_assets) },
                    { label: 'Total Debt', value: formatCurrency(fundamentals.balance_sheet_summary.total_debt) },
                    { label: 'Net Worth', value: formatCurrency(fundamentals.balance_sheet_summary.net_worth) },
                    { label: 'Cash & Equiv.', value: formatCurrency(fundamentals.balance_sheet_summary.cash_equivalents) },
                    { label: 'Book Value/Share', value: fundamentals.balance_sheet_summary.book_value_per_share?.toFixed(2) },
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
            onClick={() => setActiveTab(tab.key as TabKey)}
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
