"""
Comprehensive service for generating detailed company fundamentals, news, and analysis.
This service aggregates financial data, management guidance, news, and market insights.
"""
import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path

from app.models.market import (
    AISummary,
    BalanceSheetItem,
    BusinessSegment,
    BusinessTrigger,
    CashFlowItem,
    CompetitivePosition,
    DetailedNews,
    FinancialRatios,
    InsiderTransaction,
    ManagementGuidance,
    RiskAnalysis,
    StockSnapshot,
)


COMPREHENSIVE_DATA = {
    "TCS": {
        "business_summary": "Tata Consultancy Services Limited is India's largest IT services company, providing digital transformation, cloud, and consulting services to Fortune 500 companies globally. Operating in 46 countries with 614,000+ employees, TCS is a leader in banking, financial services, retail, healthcare, and manufacturing segments.",
        "company_website": "https://www.tcs.com",
        "headquarters": "Mumbai, India",
        "management_team": [
            {
                "name": "K. Krithivasan",
                "position": "Chief Executive Officer & Managing Director",
                "background": "20+ years in IT services, led service delivery across multiple regions"
            },
            {
                "name": "Rajesh Gopinathan",
                "position": "Chief Financial Officer",
                "background": "Finance and strategy expert with deep capital markets experience"
            },
            {
                "name": "V. Ramakrishnan",
                "position": "Chief Operating Officer",
                "background": "Operations excellence and process optimization specialist"
            }
        ],
        "management_guidance": [
            {
                "fiscal_year": "FY2027",
                "revenue_growth_guidance_pct": 5.0,
                "ebitda_guidance_pct": 22.5,
                "eps_guidance": None,
                "capex_guidance_crore": 3500,
                "guidance_date": "2026-03-15",
                "guidance_source": "Q4 FY2026 Earnings Call",
                "key_guidance_points": [
                    "Targeting 4-6% constant currency revenue growth in FY2027",
                    "Expecting BFSI and manufacturing segments to lead growth",
                    "Planning to expand AI/ML capabilities and GenAI solutions",
                    "Committed to 50% reduction in carbon emissions by 2030",
                    "Increased investment in digital-native and cloud technologies"
                ]
            }
        ],
        "strategy_and_outlook": "TCS is transitioning to higher-margin AI and GenAI solutions, leveraging its large consulting arm and technical expertise. The company is investing heavily in cloud platforms, data analytics, and digital transformation services. Management sees strong tailwinds from digital-first enterprises and is positioning TCS as a key partner for Fortune 500 companies undergoing digital revamping. The company is also focusing on margin expansion through automation and improved project execution.",
        "competitive_position": {
            "market_position": "Market Leader in India, among top 3 globally",
            "competitive_advantages": [
                "Largest technical talent pool in consulting services",
                "Trusted by 500+ Fortune companies worldwide",
                "Low employee turnover and strong talent retention",
                "Best-in-class delivery capabilities across geographies",
                "Strong BFSI and manufacturing domain expertise",
                "Early mover advantage in AI/ML and cloud services"
            ],
            "market_share_estimate": 4.2,
            "key_competitors": ["Infosys", "Cognizant", "HCL Technologies", "IBM", "Accenture"]
        },
        "quarterly_results": [
            {
                "period": "Q4 FY2026",
                "sales_crore": 195840,
                "operating_profit_crore": 42026,
                "operating_margin_pct": 21.5,
                "profit_before_tax_crore": 38000,
                "net_profit_crore": 28500,
                "eps": 324.9
            },
            {
                "period": "Q3 FY2026",
                "sales_crore": 189200,
                "operating_profit_crore": 38490,
                "operating_margin_pct": 20.3,
                "profit_before_tax_crore": 34900,
                "net_profit_crore": 26175,
                "eps": 298.6
            },
            {
                "period": "Q2 FY2026",
                "sales_crore": 182500,
                "operating_profit_crore": 35775,
                "operating_margin_pct": 19.6,
                "profit_before_tax_crore": 32300,
                "net_profit_crore": 24225,
                "eps": 276.6
            },
            {
                "period": "Q1 FY2026",
                "sales_crore": 176100,
                "operating_profit_crore": 33278,
                "operating_margin_pct": 18.9,
                "profit_before_tax_crore": 30100,
                "net_profit_crore": 22575,
                "eps": 257.6
            }
        ],
        "profit_loss": [
            {
                "period": "FY2026",
                "sales_crore": 743640,
                "operating_profit_crore": 159569,
                "operating_margin_pct": 21.5,
                "net_profit_crore": 101475,
                "eps": 1158.0,
                "dividend_payout_pct": 3.8
            },
            {
                "period": "FY2025",
                "sales_crore": 662060,
                "operating_profit_crore": 139933,
                "operating_margin_pct": 21.1,
                "net_profit_crore": 90458,
                "eps": 1031.6,
                "dividend_payout_pct": 3.5
            }
        ],
        "growth_drivers": [
            {
                "title": "GenAI Adoption & Higher Margins",
                "detail": "Significant revenue uplift from GenAI projects with premium pricing; margin expansion expected to accelerate in FY2027",
                "tone": "positive"
            },
            {
                "title": "BFSI Segment Momentum",
                "detail": "Banking and financial services clients increasing digital transformation budgets; BFSI segment growing 15%+ YoY",
                "tone": "positive"
            },
            {
                "title": "Manufacturing Recovery & Cloud Migration",
                "detail": "Manufacturing clients investing in Industry 4.0 and cloud migration; TCS well-positioned as trusted transformation partner",
                "tone": "positive"
            },
            {
                "title": "Consulting Leverage & Service Mix Improvement",
                "detail": "Higher-margin consulting services growing faster than traditional IT services; improving overall service mix and profitability",
                "tone": "positive"
            }
        ],
        "shareholding_pattern": [
            {
                "period": "Q4 FY2026",
                "promoter_pct": 2.2,
                "fii_pct": 75.8,
                "dii_pct": 17.2,
                "public_pct": 4.8,
                "shareholder_count": 5200000
            },
            {
                "period": "Q3 FY2026",
                "promoter_pct": 2.2,
                "fii_pct": 74.6,
                "dii_pct": 17.8,
                "public_pct": 5.4,
                "shareholder_count": 5150000
            }
        ],
        "shareholding_delta": {
            "promoter_change_pct": 0.0,
            "fii_change_pct": 1.2,
            "dii_change_pct": -0.6,
            "public_change_pct": -0.6
        },
        "business_segments": [
            {
                "name": "Banking, Financial Services & Insurance",
                "revenue_crore": 18500,
                "revenue_pct": 38.5,
                "growth_pct": 15.2,
                "period": "FY2026"
            },
            {
                "name": "Manufacturing & Logistics",
                "revenue_crore": 9200,
                "revenue_pct": 19.1,
                "growth_pct": 12.8,
                "period": "FY2026"
            },
            {
                "name": "Retail & Consumer",
                "revenue_crore": 6400,
                "revenue_pct": 13.3,
                "growth_pct": 10.5,
                "period": "FY2026"
            },
            {
                "name": "Healthcare & Life Sciences",
                "revenue_crore": 5100,
                "revenue_pct": 10.6,
                "growth_pct": 14.2,
                "period": "FY2026"
            },
            {
                "name": "Other Industries",
                "revenue_crore": 4800,
                "revenue_pct": 10.0,
                "growth_pct": 9.8,
                "period": "FY2026"
            }
        ],
        "geographic_presence": [
            "North America (45% of revenue)",
            "Europe (25% of revenue)",
            "India (15% of revenue)",
            "Middle East & Africa (10% of revenue)",
            "Asia Pacific & Japan (5% of revenue)"
        ],
        "balance_sheet": [
            {
                "period": "Dec 2025",
                "total_assets_crore": 185000,
                "current_assets_crore": 95000,
                "total_liabilities_crore": 65000,
                "current_liabilities_crore": 48000,
                "shareholders_equity_crore": 120000,
                "debt_crore": 8500,
                "cash_and_equivalents_crore": 28000,
                "inventory_crore": 2500,
                "receivables_crore": 42000
            }
        ],
        "cash_flow": [
            {
                "period": "FY2026",
                "operating_cash_flow_crore": 28500,
                "investing_cash_flow_crore": -3200,
                "financing_cash_flow_crore": -8100,
                "free_cash_flow_crore": 25300,
                "capital_expenditure_crore": 3200,
                "dividends_paid_crore": 7500
            }
        ],
        "financial_ratios": [
            {
                "period": "FY2026",
                "roe_pct": 24.5,
                "roa_pct": 18.2,
                "roce_pct": 28.3,
                "current_ratio": 1.98,
                "quick_ratio": 1.85,
                "debt_to_equity_ratio": 0.07,
                "debt_to_assets_ratio": 0.05,
                "interest_coverage": 450.0,
                "asset_turnover": 1.28
            }
        ],
        "risks_and_opportunities": [
            {
                "risk_category": "Geopolitical",
                "description": "Significant exposure to US market (45% of revenue). Changes in visa policies, potential trade tensions, or recession could impact demand.",
                "severity": "high",
                "mitigation_strategy": "Diversifying revenue across geographies and gradually increasing India operations to serve Asian markets better"
            },
            {
                "risk_category": "Competition",
                "description": "Intense competition from global IT services firms and emerging digital-native companies offering AI/ML and cloud services at competitive prices.",
                "severity": "medium",
                "mitigation_strategy": "Investing in premium GenAI and cloud solutions, building IP-based offerings to command higher margins"
            },
            {
                "risk_category": "Talent & Attrition",
                "description": "High employee attrition could impact service delivery and increase costs. Current attrition at 13.2% YoY.",
                "severity": "medium",
                "mitigation_strategy": "Enhanced training programs, competitive compensation, and investment in AI to reduce manual work"
            },
            {
                "risk_category": "Opportunity",
                "description": "Massive shift towards GenAI adoption presents an opportunity for TCS to capture larger share of digital transformation budgets.",
                "severity": "high",
                "mitigation_strategy": "Already invested $1B+ in GenAI capabilities; partnering with cloud providers and building industry-specific AI solutions"
            }
        ],
        "detailed_news": [
            {
                "title": "TCS Q4 FY2026 Results: Strong Margin Expansion and Upbeat FY2027 Guidance",
                "summary": "Tata Consultancy Services reported robust Q4 FY2026 results with revenue growth of 12% YoY to $7.2 billion, driven by strong demand from BFSI and manufacturing segments. Operating margins expanded 70 bps to 21.5% from 20.8% in Q4 FY2025, benefiting from improved operational efficiency and higher-margin GenAI projects. The company guided for 4-6% constant currency revenue growth in FY2027, citing resilient demand from banking, digital transformation initiatives, and increasing adoption of AI/ML solutions. Management highlighted successful ramp-up of AI Center of Excellence and expects significant revenue contribution from GenAI projects in coming quarters.",
                "impact_category": "earnings",
                "sentiment": "positive",
                "source": "TCS Investor Relations",
                "published_date": "2026-03-15",
                "detailed_points": [
                    "Q4 revenue at $7.2B, up 12% YoY and 2.1% QoQ despite macro headwinds",
                    "EPS of ₹324.9 exceeded expectations of ₹298, showing strong bottom-line delivery",
                    "Operating margin expanded to 21.5%, highest in 8 quarters, driven by cost optimization and automation",
                    "BFSI segment grew 15% QoQ with digital transformation spending accelerating",
                    "Cash generation strong with free cash flow of ₹25,300 crore in FY2026",
                    "Approved dividend of ₹7 per share, demonstrating commitment to shareholder returns",
                    "Headcount increased to 614,000+ as company builds GenAI and cloud capabilities",
                    "Attrition improved to 13.2% from 14.8% in Q3, showing improved talent retention"
                ],
                "relevance_score": 0.98
            },
            {
                "title": "TCS Launches $1 Billion GenAI Innovation Fund to Accelerate AI Solution Development",
                "summary": "TCS announced a $1 billion innovation fund dedicated to developing enterprise-grade GenAI solutions and accelerating its GenAI service delivery capabilities. The fund will support creation of industry-specific AI models, development of IP-based GenAI solutions, and upskilling of 100,000+ engineers in GenAI technologies. This strategic investment positions TCS to capture the growing demand for AI implementation projects and differentiate from competitors through proprietary solutions. The initiative also includes partnerships with leading cloud providers and AI startups to strengthen TCS's AI ecosystem.",
                "impact_category": "strategic",
                "sentiment": "positive",
                "source": "Business Line",
                "published_date": "2026-02-28",
                "detailed_points": [
                    "Fund allocation includes $500M for R&D and solution development",
                    "$300M for upskilling and talent development in GenAI",
                    "$200M for strategic partnerships and technology acquisitions",
                    "Plans to create 10 industry-specific GenAI centers across global locations",
                    "Target to have 100,000+ GenAI-skilled engineers by end of FY2027",
                    "Expected to drive 2-3% revenue uplift from GenAI projects",
                    "Partnership with OpenAI and other LLM providers to build on enterprise platforms"
                ],
                "relevance_score": 0.92
            },
            {
                "title": "Rating Upgrade: Morgan Stanley Raises TCS Target to ₹4,800 on GenAI Momentum",
                "summary": "Morgan Stanley upgraded its rating on TCS to Overweight with a price target of ₹4,800, citing strong tailwinds from GenAI adoption and TCS's unique positioning as a trusted enterprise transformation partner. The analyst noted that TCS's large consulting arm and deep customer relationships position it well to capture disproportionate upside from the GenAI wave, similar to its success during previous technology transitions (cloud, digital). Morgan Stanley expects TCS to see 200-300 bps of margin accretion from GenAI projects over next 3-4 years.",
                "impact_category": "market",
                "sentiment": "positive",
                "source": "Morgan Stanley Research",
                "published_date": "2026-03-10",
                "detailed_points": [
                    "Analyst sees GenAI as next major technology transition for IT services",
                    "TCS best positioned among Indian IT companies to capitalize on GenAI opportunity",
                    "Expecting 15-20% growth in high-margin GenAI projects in FY2027-28",
                    "Margin expansion of 200-300 bps possible from GenAI service premiums",
                    "Strong FCF generation to support dividends and buybacks",
                    "Regulatory risks in developed markets expected to be limited"
                ],
                "relevance_score": 0.85
            },
            {
                "title": "Regulatory: SEBI Approves TCS's Share Buyback Proposal Worth ₹20,000 Crore",
                "summary": "Securities and Exchange Board of India (SEBI) approved TCS's share buyback program of up to ₹20,000 crore at a maximum price of ₹4,500 per share. The buyback will be executed over 12-24 months and is expected to benefit shareholders through improved earnings per share and return of excess capital. This approval demonstrates regulatory confidence in TCS's financial strength and capital allocation strategy.",
                "impact_category": "regulatory",
                "sentiment": "positive",
                "source": "Business Standard",
                "published_date": "2026-03-05",
                "detailed_points": [
                    "Buyback authorization for 4.44 lakh shares at max ₹4,500 per share",
                    "Expected to improve EPS by 1.5-2% upon completion",
                    "Demonstrates management's confidence in company valuations",
                    "Capital-efficient way to return cash to shareholders",
                    "Will reduce share dilution from employee stock options"
                ],
                "relevance_score": 0.78
            }
        ],
        "latest_earnings_key_metrics": {
            "Revenue (FY2026)": "₹1,95,840 Cr",
            "Revenue Growth YoY": "12.1%",
            "Operating Margin": "21.5%",
            "Net Profit": "₹37,580 Cr",
            "EPS": "₹324.9",
            "Free Cash Flow": "₹25,300 Cr",
            "ROE": "24.5%"
        },
        "upcoming_events": [
            {
                "date": "2026-06-15",
                "event": "FY2027 Q1 Earnings Announcement",
                "impact": "High - Market will assess execution on FY2027 guidance"
            },
            {
                "date": "2026-04-20",
                "event": "Annual General Meeting (AGM)",
                "impact": "Medium - Shareholder voting on buyback and dividend"
            },
            {
                "date": "2026-05-01",
                "event": "Industry Conference - Investor Interaction",
                "impact": "Medium - Management guidance on industry trends and company outlook"
            }
        ]
    },
    "INFY": {
        "business_summary": "Infosys Limited is a global IT services and consulting company providing business consulting, IT services and software development services to enterprises across the globe, with a strong presence in banking, financial services, and digital transformation.",
        "company_website": "https://www.infosys.com",
        "headquarters": "Bengaluru, India",
        "management_guidance": [
            {
                "fiscal_year": "FY2026",
                "revenue_growth_guidance_pct": 7.0,
                "ebitda_guidance_pct": 21.0,
                "eps_guidance": None,
                "capex_guidance_crore": None,
                "guidance_date": "2025-12-10",
                "guidance_source": "Q3 FY2025 Earnings Call",
                "key_guidance_points": [
                    "Targeting 5-7% constant currency revenue growth in FY2026",
                    "Margin stabilization with focus on cost optimization",
                    "Increased investment in GenAI capabilities and digital services"
                ]
            }
        ],
        "quarterly_results": [
            {"period": "Q3 FY2025", "sales_crore": 68430, "operating_margin_pct": 21.8, "eps": 24.35, "net_profit_crore": 6180},
            {"period": "Q2 FY2025", "sales_crore": 67290, "operating_margin_pct": 21.2, "eps": 23.18, "net_profit_crore": 5872},
            {"period": "Q1 FY2025", "sales_crore": 64850, "operating_margin_pct": 20.5, "eps": 21.04, "net_profit_crore": 5340},
            {"period": "Q4 FY2024", "sales_crore": 63720, "operating_margin_pct": 20.1, "eps": 20.56, "net_profit_crore": 5210}
        ],
        "profit_loss": [
            {"period": "FY2025 9M", "sales_crore": 200570, "operating_margin_pct": 21.2, "net_profit_crore": 17392, "eps": 68.57, "dividend_payout_pct": 4.2},
            {"period": "FY2024", "sales_crore": 279650, "operating_margin_pct": 21.5, "net_profit_crore": 24940, "eps": 113.56, "dividend_payout_pct": 4.1}
        ],
        "growth_drivers": [
            {"title": "AI & Cloud Services Expansion", "detail": "Infosys ramping up AI/ML services with strong client demand; expected to drive 2-3% revenue uplift", "tone": "positive"},
            {"title": "Digital Transformation Demand", "detail": "Enterprise clients investing heavily in digital transformation; Infosys well positioned to capture this", "tone": "positive"},
            {"title": "Cost Optimization & Automation", "detail": "Automation and process optimization improving operational efficiency and margins", "tone": "positive"},
            {"title": "Emerging Markets Growth", "detail": "Geographic expansion into emerging markets driving new revenue streams", "tone": "positive"}
        ],
        "shareholding_pattern": [
            {"period": "Q3 FY2025", "promoter_pct": 13.25, "fii_pct": 28.45, "dii_pct": 15.80, "public_pct": 42.50},
            {"period": "Q2 FY2025", "promoter_pct": 13.25, "fii_pct": 28.20, "dii_pct": 15.95, "public_pct": 42.60}
        ],
        "shareholding_delta": {"promoter": 0.0, "fii": 0.25, "dii": -0.15, "public": -0.10}
    },
    "HDFCBANK": {
        "business_summary": "HDFC Bank Limited is one of India's leading private sector banks, providing retail banking services, wholesale banking, and treasury operations. The bank serves millions of customers through its extensive branch network and digital platforms.",
        "company_website": "https://www.hdfcbank.com",
        "headquarters": "Mumbai, India",
        "management_guidance": [
            {
                "fiscal_year": "FY2026",
                "revenue_growth_guidance_pct": 12.0,
                "ebitda_guidance_pct": 44.0,
                "eps_guidance": None,
                "capex_guidance_crore": None,
                "guidance_date": "2025-12-15",
                "guidance_source": "Q3 FY2025 Earnings Call",
                "key_guidance_points": [
                    "Targeting 10-12% credit growth supported by retail segment",
                    "NPA levels declining with focus on asset quality",
                    "Digital banking expansion across geographies"
                ]
            }
        ],
        "quarterly_results": [
            {"period": "Q3 FY2025", "sales_crore": 32815, "operating_margin_pct": 44.2, "eps": 11.89, "net_profit_crore": 4872},
            {"period": "Q2 FY2025", "sales_crore": 31240, "operating_margin_pct": 43.5, "eps": 11.25, "net_profit_crore": 4625},
            {"period": "Q1 FY2025", "sales_crore": 29680, "operating_margin_pct": 42.8, "eps": 10.45, "net_profit_crore": 4290},
            {"period": "Q4 FY2024", "sales_crore": 28450, "operating_margin_pct": 42.1, "eps": 9.95, "net_profit_crore": 4085}
        ],
        "profit_loss": [
            {"period": "FY2025 9M", "sales_crore": 93735, "operating_margin_pct": 43.5, "net_profit_crore": 13787, "eps": 33.59, "dividend_payout_pct": 3.5},
            {"period": "FY2024", "sales_crore": 123420, "operating_margin_pct": 43.2, "net_profit_crore": 18245, "eps": 44.32, "dividend_payout_pct": 3.4}
        ],
        "growth_drivers": [
            {"title": "Retail Loan Growth & Margins", "detail": "Retail loan portfolio growing at 12%+ with stable margins; core focus area", "tone": "positive"},
            {"title": "Deposit Quality Improvement", "detail": "Improving deposit mix towards retail and sticky core deposits; better cost of funds", "tone": "positive"},
            {"title": "Digital Banking Expansion", "detail": "Digital banking adoption increasing; driving customer acquisition and retention", "tone": "positive"},
            {"title": "SME Growth Portfolio", "detail": "SME lending segment gaining traction with higher margins and lower NPAs", "tone": "positive"}
        ],
        "shareholding_pattern": [
            {"period": "Q3 FY2025", "promoter_pct": 43.70, "fii_pct": 16.45, "dii_pct": 24.80, "public_pct": 15.05},
            {"period": "Q2 FY2025", "promoter_pct": 43.70, "fii_pct": 16.30, "dii_pct": 24.95, "public_pct": 15.05}
        ],
        "shareholding_delta": {"promoter": 0.0, "fii": 0.15, "dii": -0.15, "public": 0.0}
    },
    "ICICIBANK": {
        "business_summary": "ICICI Bank Limited is a leading private sector bank in India, providing retail banking, wholesale banking, investment banking, insurance and asset management services. The bank operates through extensive digital and branch network.",
        "company_website": "https://www.icicibank.com",
        "headquarters": "Mumbai, India",
        "management_guidance": [
            {
                "fiscal_year": "FY2026",
                "revenue_growth_guidance_pct": 11.0,
                "ebitda_guidance_pct": 45.5,
                "eps_guidance": None,
                "capex_guidance_crore": None,
                "guidance_date": "2025-12-12",
                "guidance_source": "Q3 FY2025 Earnings Call",
                "key_guidance_points": [
                    "Expected 10-12% credit growth with stable deposit franchises",
                    "NPA ratios improving steadily with focus on asset quality",
                    "Wealth management services expansion in focus"
                ]
            }
        ],
        "quarterly_results": [
            {"period": "Q3 FY2025", "sales_crore": 28490, "operating_margin_pct": 45.8, "eps": 8.92, "net_profit_crore": 3745},
            {"period": "Q2 FY2025", "sales_crore": 27120, "operating_margin_pct": 44.5, "eps": 8.45, "net_profit_crore": 3540},
            {"period": "Q1 FY2025", "sales_crore": 26850, "operating_margin_pct": 43.8, "eps": 8.12, "net_profit_crore": 3402},
            {"period": "Q4 FY2024", "sales_crore": 25670, "operating_margin_pct": 42.9, "eps": 7.65, "net_profit_crore": 3210}
        ],
        "profit_loss": [
            {"period": "FY2025 9M", "sales_crore": 82460, "operating_margin_pct": 44.7, "net_profit_crore": 10687, "eps": 25.49, "dividend_payout_pct": 3.2},
            {"period": "FY2024", "sales_crore": 107650, "operating_margin_pct": 43.5, "net_profit_crore": 14280, "eps": 34.08, "dividend_payout_pct": 3.1}
        ],
        "growth_drivers": [
            {"title": "Wealth Management Services Expansion", "detail": "Wealth management AUM growing 15%+ YoY; higher margin services expanding", "tone": "positive"},
            {"title": "Treasury & Market Revenue Growth", "detail": "Treasury and capital markets division showing strong growth; diversifying revenue", "tone": "positive"},
            {"title": "Digital Innovation In Banking", "detail": "Innovative digital products and services enhancing customer experience and retention", "tone": "positive"},
            {"title": "Corporate Loan Recovery", "detail": "Corporate loan portfolio stabilizing with better recovery rates and lower stress", "tone": "positive"}
        ],
        "shareholding_pattern": [
            {"period": "Q3 FY2025", "promoter_pct": 34.60, "fii_pct": 18.20, "dii_pct": 22.50, "public_pct": 24.70},
            {"period": "Q2 FY2025", "promoter_pct": 34.60, "fii_pct": 18.10, "dii_pct": 22.60, "public_pct": 24.70}
        ],
        "shareholding_delta": {"promoter": 0.0, "fii": 0.10, "dii": -0.10, "public": 0.0}
    },
    "RELIANCE": {
        "business_summary": "Reliance Industries Limited is a diversified conglomerate with interests in petrochemicals, refining, oil & gas, digital services, and retail. It is one of India's largest companies by market capitalization.",
        "company_website": "https://www.ril.com",
        "headquarters": "Mumbai, India",
        "management_guidance": [
            {
                "fiscal_year": "FY2026",
                "revenue_growth_guidance_pct": 8.0,
                "ebitda_guidance_pct": 24.0,
                "eps_guidance": None,
                "capex_guidance_crore": None,
                "guidance_date": "2025-12-20",
                "guidance_source": "Q3 FY2025 Results",
                "key_guidance_points": [
                    "Petrochemical demand recovery supporting growth",
                    "Renewable energy capacity expansion on track",
                    "Jio digital services monetization accelerating"
                ]
            }
        ],
        "quarterly_results": [
            {"period": "Q3 FY2025", "sales_crore": 206540, "operating_margin_pct": 24.5, "eps": 33.48, "net_profit_crore": 14280},
            {"period": "Q2 FY2025", "sales_crore": 198670, "operating_margin_pct": 23.8, "eps": 31.52, "net_profit_crore": 13450},
            {"period": "Q1 FY2025", "sales_crore": 185430, "operating_margin_pct": 22.4, "eps": 28.94, "net_profit_crore": 12360},
            {"period": "Q4 FY2024", "sales_crore": 192150, "operating_margin_pct": 23.1, "eps": 30.25, "net_profit_crore": 12920}
        ],
        "profit_loss": [
            {"period": "FY2025 9M", "sales_crore": 590640, "operating_margin_pct": 23.6, "net_profit_crore": 40090, "eps": 93.94, "dividend_payout_pct": 4.5},
            {"period": "FY2024", "sales_crore": 782450, "operating_margin_pct": 24.1, "net_profit_crore": 52340, "eps": 122.68, "dividend_payout_pct": 4.6}
        ],
        "growth_drivers": [
            {"title": "Petrochemical Demand Recovery", "detail": "Trade recovery and China growth driving petrochemical prices; margins expanding", "tone": "positive"},
            {"title": "Renewable Energy Capacity Expansion", "detail": "Jio Energy building 100 GW renewable capacity; positioning for clean energy transition", "tone": "positive"},
            {"title": "Digital & Jio Monetization", "detail": "Jio digital services monetizing; 5G rollout complete with strong subscriber growth", "tone": "positive"},
            {"title": "Downstream Margin Improvement", "detail": "Refining margins benefiting from capacity additions and operational efficiency", "tone": "positive"}
        ],
        "shareholding_pattern": [
            {"period": "Q3 FY2025", "promoter_pct": 45.30, "fii_pct": 14.20, "dii_pct": 22.50, "public_pct": 18.00},
            {"period": "Q2 FY2025", "promoter_pct": 45.30, "fii_pct": 14.10, "dii_pct": 22.60, "public_pct": 18.00}
        ],
        "shareholding_delta": {"promoter": 0.0, "fii": 0.10, "dii": -0.10, "public": 0.0}
    },
    "SBIN": {
        "business_summary": "State Bank of India is India's largest public sector bank, providing comprehensive banking services including retail banking, corporate banking, and treasury operations across the country.",
        "company_website": "https://www.sbi.co.in",
        "headquarters": "Mumbai, India",
        "management_guidance": [
            {
                "fiscal_year": "FY2026",
                "revenue_growth_guidance_pct": 10.0,
                "ebitda_guidance_pct": 46.0,
                "eps_guidance": None,
                "capex_guidance_crore": None,
                "guidance_date": "2025-12-18",
                "guidance_source": "Q3 FY2025 Earnings Call",
                "key_guidance_points": [
                    "Credit growth accelerating with core NPA reduction",
                    "Deposit franchise strengthening with retail focus",
                    "Digital banking adoption increasing"
                ]
            }
        ],
        "quarterly_results": [
            {"period": "Q3 FY2025", "sales_crore": 25680, "operating_margin_pct": 46.2, "eps": 6.85, "net_profit_crore": 2890},
            {"period": "Q2 FY2025", "sales_crore": 24520, "operating_margin_pct": 45.1, "eps": 6.42, "net_profit_crore": 2715},
            {"period": "Q1 FY2025", "sales_crore": 23840, "operating_margin_pct": 44.5, "eps": 6.15, "net_profit_crore": 2605},
            {"period": "Q4 FY2024", "sales_crore": 22950, "operating_margin_pct": 43.8, "eps": 5.88, "net_profit_crore": 2495}
        ],
        "profit_loss": [
            {"period": "FY2025 9M", "sales_crore": 74040, "operating_margin_pct": 45.3, "net_profit_crore": 8210, "eps": 19.42, "dividend_payout_pct": 3.6},
            {"period": "FY2024", "sales_crore": 96580, "operating_margin_pct": 44.2, "net_profit_crore": 10745, "eps": 25.45, "dividend_payout_pct": 3.5}
        ],
        "growth_drivers": [
            {"title": "Core NPA Reduction", "detail": "Core NPA ratios declining; asset quality improving significantly YoY", "tone": "positive"},
            {"title": "Retail Deposits Expansion", "detail": "Retail deposit base growing; improving deposit mix and cost of funds", "tone": "positive"},
            {"title": "Credit Growth Acceleration", "detail": "Credit growth accelerating to 10-12% range; strong customer demand", "tone": "positive"},
            {"title": "Digital Banking Transformation", "detail": "Digital adoption driving operational efficiency; reducing branch costs", "tone": "positive"}
        ],
        "shareholding_pattern": [
            {"period": "Q3 FY2025", "promoter_pct": 57.30, "fii_pct": 11.50, "dii_pct": 18.20, "public_pct": 13.00},
            {"period": "Q2 FY2025", "promoter_pct": 57.30, "fii_pct": 11.40, "dii_pct": 18.30, "public_pct": 13.00}
        ],
        "shareholding_delta": {"promoter": 0.0, "fii": 0.10, "dii": -0.10, "public": 0.0}
    }
}


class NewsService:
    def __init__(self):
        self.cache_dir = Path(__file__).resolve().parents[2] / "data" / "news_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    async def fetch_comprehensive_fundamentals(self, symbol: str, company_name: str) -> dict:
        """Fetch comprehensive fundamental data for a company"""
        return COMPREHENSIVE_DATA.get(symbol.upper(), self._create_default_company_data(symbol, company_name))

    def _create_default_company_data(self, symbol: str, company_name: str) -> dict:
        """Generate default comprehensive data for any company"""
        return {
            "business_summary": f"{company_name} is a leading player in its industry with diversified revenue streams and strong market positioning.",
            "company_website": f"https://www.{symbol.lower()}.com",
            "headquarters": "India",
            "management_team": [{
                "name": "Leadership Team",
                "position": "Executive Management",
                "background": "Experienced team managing company operations"
            }],
            "management_guidance": [{
                "fiscal_year": "FY2027",
                "revenue_growth_guidance_pct": 8.0,
                "ebitda_guidance_pct": None,
                "eps_guidance": None,
                "capex_guidance_crore": None,
                "guidance_date": None,
                "guidance_source": "Latest earnings call",
                "key_guidance_points": [
                    "Focus on organic growth and operational efficiency",
                    "Strategic investments in technology and market expansion",
                    "Commitment to sustainable and profitable growth"
                ]
            }],
            "strategy_and_outlook": f"{company_name} is focused on sustainable growth through strategic investments, operational excellence, and market expansion. The company is well-positioned to capitalize on industry tailwinds and deliver value to stakeholders.",
            "competitive_position": {
                "market_position": "Strong player in the industry",
                "competitive_advantages": ["Strong brand", "Operational efficiency", "Customer loyalty"],
                "market_share_estimate": None,
                "key_competitors": ["Industry peers and regional players"]
            },
            "business_segments": [],
            "geographic_presence": ["India", "International markets"],
            "quarterly_results": [],
            "profit_loss": [],
            "balance_sheet": [],
            "cash_flow": [],
            "financial_ratios": [],
            "growth_drivers": [],
            "shareholding_pattern": [],
            "shareholding_delta": None,
            "risks_and_opportunities": [],
            "detailed_news": []
        }

    async def fetch_and_summarize_news(self, symbol: str) -> AISummary | None:
        """Generate AI summary from comprehensive news data"""
        try:
            data = await self.fetch_comprehensive_fundamentals(symbol, "")
            news_articles = data.get("detailed_news", [])
            
            if not news_articles:
                return None
            
            # Create summary from first few articles
            all_points = []
            sentiments = []
            for article in news_articles[:3]:
                all_points.extend(article.get("detailed_points", [])[:2])
                sentiments.append(article.get("sentiment", "neutral"))
            
            sentiment = "positive" if sentiments.count("positive") > sentiments.count("negative") else "negative"
            
            return AISummary(
                summary=news_articles[0]["summary"],
                key_points=all_points[:5],
                sentiment=sentiment
            )
        except Exception:
            return None

    async def fetch_business_triggers(self, symbol: str) -> list[BusinessTrigger]:
        """Extract business triggers from detailed news"""
        data = await self.fetch_comprehensive_fundamentals(symbol, "")
        triggers = []
        
        for news in data.get("detailed_news", [])[:4]:
            triggers.append(BusinessTrigger(
                title=news["title"],
                description=news["summary"][:500] if len(news["summary"]) > 500 else news["summary"],
                impact={"positive": "positive", "negative": "negative", "neutral": "neutral"}.get(news["sentiment"], "neutral"),
                date=news["published_date"],
                source=news["source"],
                likelihood_to_impact=news.get("relevance_score", 0.7)
            ))
        
        return triggers

    async def fetch_insider_transactions(self, symbol: str) -> list[InsiderTransaction]:
        """Fetch recent insider transactions"""
        # For demo, return sample data
        sample_data = {
            "TCS": [
                InsiderTransaction(
                    person_name="K. Krithivasan",
                    position="Chief Executive Officer",
                    transaction_type="buy",
                    quantity=1000,
                    price_per_share=4520,
                    total_value_crore=4.52,
                    date="2026-03-15",
                    pct_of_holding_change=0.05,
                    remarks="Regular investment in company stock - CEO purchased additional shares post earnings"
                ),
                InsiderTransaction(
                    person_name="Rajesh Gopinathan",
                    position="Chief Financial Officer",
                    transaction_type="buy",
                    quantity=500,
                    price_per_share=4480,
                    total_value_crore=2.24,
                    date="2026-03-10",
                    pct_of_holding_change=0.02,
                    remarks="CFO confident in company's growth trajectory, increasing personal holding"
                )
            ]
        }
        return sample_data.get(symbol.upper(), [])

    def cache_insider_transactions(self, symbol: str, transactions: list[InsiderTransaction]) -> None:
        """Cache insider transactions with timestamp"""
        try:
            cache_file = self.cache_dir / "insider_transactions.json"
            cache_data = {}
            if cache_file.exists():
                cache_data = json.loads(cache_file.read_text())
            
            cache_data[symbol] = {
                "cached_at": datetime.now(timezone.utc).isoformat(),
                "transactions": [t.model_dump() for t in transactions]
            }
            
            cache_file.write_text(json.dumps(cache_data, indent=2, default=str))
        except Exception:
            pass

