import type { IndustryGroupStockItem, MarketKey } from "../lib/api";

type ChartGroupMember = IndustryGroupStockItem & {
  group_member_rank: number;
};

type ChartGroupModalContext = {
  groupId: string;
  groupName: string;
  parentSector: string;
  description: string;
  groupRank: number;
  groupRankLabel: string;
  stockRank: number;
  stockCount: number;
  strengthBucket: string;
  trendLabel: string;
  symbols: string[];
  members: ChartGroupMember[];
};

type ChartGroupModalProps = {
  market: MarketKey;
  context: ChartGroupModalContext;
  selectedSymbol: string | null;
  onClose: () => void;
  onSelectSymbol: (symbol: string) => void;
  onAddToWatchlist: (symbol: string) => void;
  onOpenGroupsPage: () => void;
};

function formatReturn(value: number) {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatPrice(value: number, market: MarketKey) {
  const locale = market === "us" ? "en-US" : "en-IN";
  const symbol = market === "us" ? "$" : "₹";
  return `${symbol}${value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function metricClass(value: number) {
  return value >= 0 ? "positive-text" : "negative-text";
}

export function ChartGroupModal({
  market,
  context,
  selectedSymbol,
  onClose,
  onSelectSymbol,
  onAddToWatchlist,
  onOpenGroupsPage,
}: ChartGroupModalProps) {
  return (
    <div className="chart-group-modal-backdrop" onClick={onClose}>
      <div className="chart-group-modal" onClick={(event) => event.stopPropagation()}>
        <button type="button" className="chart-group-modal-close" onClick={onClose}>
          Close
        </button>

        <div className="chart-group-modal-header">
          <div>
            <p className="chart-group-modal-eyebrow">{context.parentSector}</p>
            <h3>{context.groupName}</h3>
            <p>{context.description}</p>
          </div>
          <div className="chart-group-modal-actions">
            <button type="button" className="tool-pill" onClick={onOpenGroupsPage}>
              Open Groups Page
            </button>
          </div>
        </div>

        <div className="chart-group-summary-strip">
          <div className="chart-group-summary-card">
            <span>Group Rank</span>
            <strong>{context.groupRankLabel}</strong>
          </div>
          <div className="chart-group-summary-card">
            <span>Selected Stock</span>
            <strong>{`${context.stockRank}/${context.stockCount}`}</strong>
          </div>
          <div className="chart-group-summary-card">
            <span>Strength</span>
            <strong>{context.strengthBucket}</strong>
          </div>
          <div className="chart-group-summary-card">
            <span>Trend</span>
            <strong>{context.trendLabel}</strong>
          </div>
        </div>

        <div className="chart-group-table-head">
          <span>Rank</span>
          <span>Stock</span>
          <span>Price</span>
          <span>1D</span>
          <span>1M</span>
          <span>3M</span>
          <span>6M</span>
          <span>RS</span>
          <span>Watch</span>
        </div>

        <div className="chart-group-table-body">
          {context.members.map((member) => {
            const isActive = member.symbol === selectedSymbol;
            return (
              <div
                key={`${context.groupId}:${member.symbol}`}
                className={isActive ? "chart-group-row active" : "chart-group-row"}
              >
                <span className="chart-group-rank">#{member.group_member_rank}</span>
                <button type="button" className="chart-group-stock-button" onClick={() => onSelectSymbol(member.symbol)}>
                  <span>
                    <strong>{member.symbol}</strong>
                    <small>{member.company_name}</small>
                  </span>
                </button>
                <span>{formatPrice(member.last_price, market)}</span>
                <span className={metricClass(member.change_pct)}>{formatReturn(member.change_pct)}</span>
                <span className={metricClass(member.return_1m)}>{formatReturn(member.return_1m)}</span>
                <span className={metricClass(member.return_3m)}>{formatReturn(member.return_3m)}</span>
                <span className={metricClass(member.return_6m)}>{formatReturn(member.return_6m)}</span>
                <span>{member.rs_rating ?? "--"}</span>
                <button type="button" className="tool-pill small" onClick={() => onAddToWatchlist(member.symbol)}>
                  Add
                </button>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

export default ChartGroupModal;