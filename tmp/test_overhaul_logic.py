from app.services.news_logic import NewsPipeline
from app.models.market import DetailedNews
import json

def test_news_pipeline():
    print("Testing News Pipeline...")
    raw_items = [
        {
            "title": "Reliance Industries announces Q4 results",
            "url": "https://www.prnewswire.com/news-releases/reliance-q4.html",
            "source": "PR Newswire",
            "snippet": "Reliance reported strong numbers..."
        },
        {
            "title": "Reliance shares surge on strong Q4 earnings",
            "url": "https://economictimes.indiatimes.com/markets/stocks/news/reliance-shares.html",
            "source": "Economic Times",
            "snippet": "Shares of RIL were trading higher..."
        },
        {
            "title": "Disclosure under Regulation 30 of SEBI",
            "url": "https://www.bseindia.com/xml-data/corpfiling/reliance.xml",
            "source": "BSE",
            "snippet": "Outcome of board meeting..."
        }
    ]
    
    editorial, official = NewsPipeline.process_and_split(raw_items)
    
    print(f"Editorial items: {len(editorial)}")
    for item in editorial:
        print(f"  - {item.title} ({item.source})")
        assert item.is_editorial == True
        
    print(f"Official items: {len(official)}")
    for item in official:
        print(f"  - {item.title} ({item.source})")
        assert item.is_editorial == False

def test_dedupe():
    print("\nTesting Deduplication...")
    items = [
        DetailedNews(title="Stock surges 10%", summary="High volume", impact_category="market", source="Reuters", relevance_score=0.9),
        DetailedNews(title="Stock surges 10% on high volume", summary="Reuters reported...", impact_category="market", source="Bloomberg", relevance_score=0.8),
        DetailedNews(title="Unique news item", summary="Something else", impact_category="strategic", source="CNBC", relevance_score=0.7)
    ]
    
    deduped = NewsPipeline.dedupe_news(items)
    print(f"Items after dedupe: {len(deduped)}")
    for item in deduped:
        print(f"  - {item.title}")
    assert len(deduped) == 2

if __name__ == "__main__":
    try:
        test_news_pipeline()
        test_dedupe()
        print("\nAll backend logic tests PASSED!")
    except Exception as e:
        print(f"\nTests FAILED: {e}")
