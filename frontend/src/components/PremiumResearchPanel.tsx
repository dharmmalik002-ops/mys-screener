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
    { key: 'overview', label: 'Overview' },
    { key: 'results', label: 'Results Explorer' },
    { key: 'fundamentals', label: 'Fundamentals' },
    { key: 'guidance', label: 'Guidance & Concall' },
    { key: 'news', label: 'News & Updates' },
    { key: 'risks', label: 'Risks & Triggers' },
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
      <section className="research-card">
        <div className="research-card-head">
          <div>
            <h3>Business Context</h3>
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
          <h4>AI Research Insight</h4>
          <p style={{ margin: '12px 0' }}>{fundamentals.ai_news_summary.summary}</p>
          <ul className="news-detailed-points">
            {fundamentals.ai_news_summary.key_points.map((p, i) => <li key={i}>{p}</li>)}
          </ul>
        </section>
      )}

      {fundamentals.growth_trends && (
        <div className="results-highlights">
          <div className="highlight-box">
            <span className="highlight-label">Revenue Trend</span>
            <div className="highlight-value">{fundamentals.growth_trends.revenue_trend}</div>
          </div>
          <div className="highlight-box">
            <span className="highlight-label">Profit Trend</span>
            <div className="highlight-value">{fundamentals.growth_trends.profit_trend}</div>
          </div>
          <div className="highlight-box">
            <span className="highlight-label">Margin Trend</span>
            <div className="highlight-value">{fundamentals.growth_trends.margin_trend}</div>
          </div>
        </div>
      )}
    </div>
  );

  const renderResults = () => (
    <div className="fade-in">
      {fundamentals.results_summary && (
        <section className="research-card">
          <div className="research-card-head">
            <h3>Latest Quarter Performance</h3>
            <div className="badge badge-impact">{fundamentals.results_summary.beat_miss}</div>
          </div>
          <div className="highlight-box" style={{ marginBottom: 16 }}>
            <h4>Segment Highlights</h4>
            <p>{fundamentals.results_summary.segment_performance}</p>
          </div>
          <ul className="news-detailed-points">
            {fundamentals.results_summary.highlights?.map((h: string, i: number) => <li key={i}>{h}</li>)}
          </ul>
        </section>
      )}

      <section className="research-card">
        <h3>Quarterly Trajectory</h3>
        <div className="fundamentals-table-wrap">
          <table className="fundamentals-table">
            <thead>
              <tr>
                <th>Quarter</th>
                <th>Revenue</th>
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
                  <td>{formatPercent(q.operating_margin_pct)}</td>
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

  const renderNews = () => (
    <div className="fade-in">
      <div className="news-feed-split">
        <div className="news-column">
          <div className="news-section-title">
            <span role="img" aria-label="journalism">📊</span> Journalism & Media News
          </div>
          {editorialNews.length > 0 ? editorialNews.map((news, i) => (
            <div key={i} className="news-article-card">
              <div className="news-article-meta">
                <span className="badge badge-editorial">{news.source}</span>
                <span>{news.published_date}</span>
                {news.impact_area && <span className="badge badge-impact">{news.impact_area}</span>}
              </div>
              <h4>{news.title}</h4>
              <p className="news-summary" style={{ fontSize: '12px', margin: '8px 0' }}>{news.summary}</p>
              {news.why_it_matters && (
                <div className="news-why-it-matters">
                  <strong>Why it matters:</strong> {news.why_it_matters}
                </div>
              )}
            </div>
          )) : <div className="empty-state">No editorial news found recently.</div>}
        </div>

        <div className="news-column">
          <div className="news-section-title">
            <span role="img" aria-label="official">🔔</span> Official Filings & Releases
          </div>
          {officialNews.length > 0 ? officialNews.map((news, i) => (
            <div key={i} className="news-article-card">
              <div className="news-article-meta">
                <span className="badge badge-official">{news.source_type}</span>
                <span>{news.source}</span>
                <span>{news.published_date}</span>
              </div>
              <h4>{news.title}</h4>
              <p className="news-summary" style={{ fontSize: '12px' }}>{news.summary}</p>
              {news.connection_to_guidance && (
                <div className="news-why-it-matters" style={{ borderColor: '#ffb347' }}>
                   {news.connection_to_guidance}
                </div>
              )}
            </div>
          )) : <div className="empty-state">No official filings detected recently.</div>}
        </div>
      </div>
    </div>
  );

  const renderGuidance = () => (
    <div className="fade-in">
      <section className="research-card">
        <h3>Management Guidance Tracker</h3>
        <div className="guidance-list">
          <div className="guidance-tracker-row guidance-header">
            <div>Date</div>
            <div>Metric</div>
            <div>Commentary</div>
            <div>Status</div>
          </div>
          {fundamentals.guidance_tracker && fundamentals.guidance_tracker.length > 0 ? fundamentals.guidance_tracker.map((g, i) => (
             <div key={i} className="guidance-tracker-row">
                <div style={{ color: 'var(--text-secondary)' }}>{g.date}</div>
                <div style={{ fontWeight: 600 }}>{g.previous}</div>
                <div>{g.current}</div>
                <div className="badge badge-impact">{g.reason}</div>
             </div>
          )) : <div className="empty-state" style={{ padding: '20px 0' }}>Detailed guidance history is being processed.</div>}
        </div>
      </section>

      <section className="research-card">
        <h3>Active Guidance</h3>
        <div className="management-guidance-list">
          {fundamentals.management_guidance.map((g, i) => (
            <div key={i} className="guidance-item" style={{ background: 'var(--bg-hover)', border: 'none', padding: '16px' }}>
              <div className="research-card-head">
                <h4>{g.fiscal_year} Outlook ({g.guidance_type})</h4>
                {g.confidence_score && (
                  <div className="badge badge-impact">Conf: {Math.round(g.confidence_score * 100)}%</div>
                )}
              </div>
              <div className="guidance-metrics" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))' }}>
                {g.revenue_growth_guidance_pct && <div>Rev Plan: {formatPercent(g.revenue_growth_guidance_pct)}</div>}
                {g.ebitda_guidance_pct && <div>EBITDA: {formatPercent(g.ebitda_guidance_pct)}</div>}
                {g.capex_guidance_crore && <div>Capex: {formatCurrency(g.capex_guidance_crore)}</div>}
              </div>
              <div className="guidance-points" style={{ marginTop: 12 }}>
                <strong>Strategic Initiatives:</strong>
                <ul className="news-detailed-points">
                  {g.key_guidance_points.map((p, j) => <li key={j}>{p}</li>)}
                </ul>
              </div>
              {g.analyst_concerns && g.analyst_concerns.length > 0 && (
                <div className="news-why-it-matters" style={{ borderColor: 'var(--downtrend-color)', background: 'rgba(255, 82, 82, 0.03)' }}>
                  <strong>Analyst Concerns:</strong> {g.analyst_concerns.join(', ')}
                </div>
              )}
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
                  <h4 style={{ marginBottom: 16 }}>Critical Risks</h4>
                  {fundamentals.risks_and_opportunities.map((r, i) => (
                    <div key={i} className="news-article-card" style={{ borderLeft: `4px solid ${r.severity === 'high' ? 'var(--downtrend-color)' : '#ffb347'}` }}>
                       <div className="news-article-meta">
                          <span className="badge badge-impact">{r.risk_category}</span>
                       </div>
                       <h4>{r.description}</h4>
                       <p className="news-summary" style={{ margin: '8px 0' }}><strong>Mitigation:</strong> {r.mitigation_strategy}</p>
                    </div>
                  ))}
               </div>
               <div className="column">
                  <h4 style={{ marginBottom: 16 }}>Business Triggers</h4>
                  {fundamentals.business_triggers.map((t, i) => (
                    <div key={i} className="news-article-card">
                       <div className="news-article-meta">
                          <span className={`sentiment-pill sentiment-${t.impact}`}>{t.impact.toUpperCase()}</span>
                          <span>Likelihood: {Math.round(t.likelihood_to_impact * 100)}%</span>
                       </div>
                       <h4>{t.title}</h4>
                       <p className="news-summary">{t.description}</p>
                    </div>
                  ))}
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
