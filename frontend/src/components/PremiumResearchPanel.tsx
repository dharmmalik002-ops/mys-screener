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

  const renderResults = () => (
    <div className="fade-in">
      {fundamentals.results_summary && (
        <section className="research-card">
          <div className="research-card-head">
            <h3>Latest Quarter Performance</h3>
            <div className={`badge badge-impact ${fundamentals.results_summary.beat_miss?.toLowerCase().includes('beat') ? 'sentiment-positive' : 'sentiment-negative'}`}>
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
                <th>EBITDA</th>
                <th>OPM %</th>
                <th>Net Profit</th>
                <th>EPS</th>
              </tr>
            </thead>
            <tbody>
              {fundamentals.quarterly_results.map(q => (
                <tr key={q.period}>
                  <td style={{ fontWeight: 600 }}>{q.period}</td>
                  <td>{formatCurrency(q.sales_crore)}</td>
                  <td>{formatCurrency(q.ebitda_crore)} <span className="small text-secondary">({q.ebitda_margin_pct}%)</span></td>
                  <td className={q.operating_margin_pct && q.operating_margin_pct >= 20 ? 'text-uptrend' : ''}>
                    {formatPercent(q.operating_margin_pct)}
                  </td>
                  <td>{formatCurrency(q.net_profit_crore)}</td>
                  <td style={{ fontWeight: 600 }}>{q.eps}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );

  const renderNews = () => {
    const editorial = fundamentals.latest_editorial_news || editorialNews;
    const official = fundamentals.official_updates || officialNews;

    return (
      <div className="fade-in">
        <div className="news-feed-split">
          <div className="news-column">
            <div className="news-section-title">
              <span role="img" aria-label="journalism">📊</span> Latest Editorial News
              <p className="text-secondary small" style={{ textTransform: 'none', marginLeft: 'auto' }}>Filtered Journalism</p>
            </div>
            {editorial.length > 0 ? editorial.map((news, i) => (
              <div key={i} className="news-article-card editorial-card">
                <div className="news-article-meta">
                  <span className="badge badge-editorial">{news.source}</span>
                  <span>{news.published_date}</span>
                  {news.impact_area && <span className="impact-indicator">{news.impact_area}</span>}
                </div>
                <h4>{news.title}</h4>
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
            )) : <div className="empty-state">No editorial news found recently.</div>}
          </div>

          <div className="news-column">
            <div className="news-section-title">
              <span role="img" aria-label="official">🔔</span> Official Company Filings
              <p className="text-secondary small" style={{ textTransform: 'none', marginLeft: 'auto' }}>Exchanges & PR</p>
            </div>
            {official.length > 0 ? official.map((news, i) => (
              <div key={i} className="news-article-card official-card">
                <div className="news-article-meta">
                  <span className="badge badge-official">{news.classification || news.source_type}</span>
                  <span className="text-secondary">{news.source}</span>
                  <span>{news.published_date}</span>
                </div>
                <h4>{news.title}</h4>
                <p className="news-summary">{news.summary}</p>
                <div className="news-footer">
                  {news.url && <a href={news.url} target="_blank" rel="noopener noreferrer" className="view-source">View Source Document</a>}
                </div>
              </div>
            )) : <div className="empty-state">No official filings detected recently.</div>}
          </div>
        </div>
      </div>
    );
  };

  const renderGuidance = () => (
    <div className="fade-in">
      <section className="research-card">
        <div className="research-card-head">
          <h3>Management Outlook / Active Guidance</h3>
          <div className="guidance-date-context text-secondary small">Current Date: April 2026</div>
        </div>
        
        <div className="management-guidance-list">
          {fundamentals.management_guidance?.length > 0 ? fundamentals.management_guidance.map((g, i) => (
            <div key={i} className={`guidance-item ${g.is_stale ? 'guidance-stale' : 'guidance-valid'}`}>
              <div className="research-card-head">
                <div className="guidance-banner-wrap">
                   <h4 className="guidance-period-label">{g.fiscal_period || g.fiscal_year} Guidance</h4>
                   <div className={`validity-banner banner-${g.validity_banner?.toLowerCase()}`}>
                     {g.validity_banner?.toUpperCase()}
                   </div>
                </div>
                <div className="guidance-meta text-secondary">
                  Source: {g.guidance_source} ({g.source_date || g.guidance_date || 'Recent'})
                </div>
              </div>

              <div className="guidance-metrics-row">
                <div className="g-metric">
                  <span className="g-label">Revenue</span>
                  <span className="g-value">{g.revenue_growth_guidance_pct ? formatPercent(g.revenue_growth_guidance_pct) : 'Qualitative'}</span>
                </div>
                <div className="g-metric">
                  <span className="g-label">EBITDA</span>
                  <span className="g-value">{g.ebitda_guidance_pct ? formatPercent(g.ebitda_guidance_pct) : 'N/A'}</span>
                </div>
                <div className="g-metric">
                  <span className="g-label">Capex</span>
                  <span className="g-value">{g.capex_guidance_crore ? formatCurrency(g.capex_guidance_crore) : 'N/A'}</span>
                </div>
              </div>

              {g.is_stale && (
                <div className="stale-warning-box">
                  ⚠️ This guidance refers to a concluded or superseded fiscal period ({g.fiscal_period}). Management commentary may have evolved.
                </div>
              )}

              <div className="guidance-points-wrap">
                <strong>Key Management Directives:</strong>
                <ul className="news-detailed-points grid-2">
                  {g.key_guidance_points.map((p, j) => <li key={j}>{p}</li>)}
                </ul>
              </div>
            </div>
          )) : (
            <div className="empty-state">No current active guidance provided by management.</div>
          )}
        </div>
      </section>

      <section className="research-card">
        <h3>Guidance Tracker (Consensus vs Management)</h3>
        <div className="guidance-list">
          <div className="guidance-tracker-row guidance-header">
            <div>Revision Date</div>
            <div>Previous Outlook</div>
            <div>New Forecast</div>
            <div>Reason for Change</div>
          </div>
          {fundamentals.guidance_tracker?.map((g, i) => (
             <div key={i} className="guidance-tracker-row">
                <div className="text-secondary">{g.date}</div>
                <div className="text-bold">{g.previous}</div>
                <div className="text-accent">{g.current}</div>
                <div className="badge-slim">{g.reason}</div>
             </div>
          ))}
        </div>
      </section>
    </div>
  );

  const renderContent = () => {
    switch (activeTab) {
      case 'overview': return renderOverview();
      case 'results': return renderResults();
      case 'news': return renderNews();
      case 'guidance': return renderGuidance();
      case 'fundamentals': 
        // We can reuse the existing table logic here or implement a better one
        return (
          <div className="fade-in">
             <section className="research-card">
                <h3>Key Ratios</h3>
                {/* Ratios Table */}
                <div className="fundamentals-table-wrap">
                  <table className="fundamentals-table">
                    <thead>
                      <tr><th>Period</th><th>ROE %</th><th>ROCE %</th><th>D/E</th><th>Current</th></tr>
                    </thead>
                    <tbody>
                      {fundamentals.financial_ratios.map(r => (
                        <tr key={r.period}>
                          <td>{r.period}</td>
                          <td>{formatPercent(r.roe_pct)}</td>
                          <td>{formatPercent(r.roce_pct)}</td>
                          <td>{r.debt_to_equity_ratio?.toFixed(2)}</td>
                          <td>{r.current_ratio?.toFixed(2)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
             </section>
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
