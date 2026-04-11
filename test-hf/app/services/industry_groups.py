from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Iterable

from app.models.market import (
    IndustryGroupFilters,
    IndustryGroupMasterItem,
    IndustryGroupRankItem,
    IndustryGroupsResponse,
    IndustryGroupStockItem,
    IndustryGroupTopStock,
    StockSnapshot,
)

GROUP_MIN_MARKET_CAP_CR = 800.0
GROUP_MIN_AVG_DAILY_VALUE_CR = 1.0
GROUP_KEEP_THRESHOLD = 5

INDIA_DIRECT_GROUP_MAP: dict[str, str] = {
    "Pharmaceuticals": "Pharma",
    "Biotechnology": "Pharma",
    "Auto Components & Equipments": "Auto Ancillaries",
    "Civil Construction": "EPC & Construction",
    "Residential Commercial Projects": "Realty Developers",
    "Non Banking Financial Company (NBFC)": "NBFCs",
    "Computers - Software & Consulting": "IT Services",
    "Aerospace & Defense": "Defence",
    "Cement & Cement Products": "Cement",
    "Private Sector Bank": "Private Banks",
    "Public Sector Bank": "PSU Banks",
    "Other Bank": "Other Banks",
    "Gems Jewellery And Watches": "Jewellery",
    "Other Textile Products": "Textiles",
    "Hospital": "Hospitals",
    "Healthcare Service Provider": "Hospitals",
    "Healthcare Research Analytics & Technology": "Hospitals",
    "Diversified Commercial Services": "Business Services",
    "Stockbroking & Allied": "Brokerages & Wealth",
    "Exchange and Data Platform": "Brokerages & Wealth",
    "Depositories Clearing Houses and Other Intermediaries": "Brokerages & Wealth",
    "Ratings": "Brokerages & Wealth",
    "Logistics Solution Provider": "Logistics",
    "Pesticides & Agrochemicals": "Agro Chemicals",
    "Garments & Apparels": "Apparel & Fashion",
    "Breweries & Distilleries": "Alcoholic Beverages",
    "Compressors Pumps & Diesel Engines": "Industrial Machinery",
    "Housing Finance Company": "Housing Finance",
    "Plastic Products - Industrial": "Industrial Plastics",
    "Speciality Retail": "Retail Specialty",
    "Packaged Foods": "FMCG Foods",
    "Other Food Products": "FMCG Foods",
    "Meat Products including Poultry": "FMCG Foods",
    "Seafood": "FMCG Foods",
    "Tea & Coffee": "FMCG Foods",
    "Other Agricultural Products": "Agri Commodities",
    "Animal Feed": "Agri Commodities",
    "Cables - Electricals": "Cables & Wires",
    "E-Retail/ E-Commerce": "E-Commerce",
    "Internet & Catalogue Retail": "E-Commerce",
    "Financial Institution": "Financial Institutions",
    "Other Financial Services": "Financial Institutions",
    "Tour Travel Related Services": "Travel & Aviation",
    "Airline": "Travel & Aviation",
    "Airport & Airport services": "Travel & Aviation",
    "Amusement Parks/ Other Recreation": "Travel & Aviation",
    "2/3 Wheelers": "Auto 2W",
    "Asset Management Company": "Asset Managers",
    "Financial Technology (Fintech)": "Fintech",
    "LPG/CNG/PNG/LNG Supplier": "Gas Utilities",
    "Gas Transmission/Marketing": "Gas Utilities",
    "Passenger Cars & Utility Vehicles": "Auto PV",
    "Refineries & Marketing": "OMCs & Refining",
    "Lubricants": "OMCs & Refining",
    "Restaurants": "Restaurants & QSR",
    "Telecom - Cellular & Fixed line services": "Telecom Services",
    "Tyres & Rubber Products": "Tyres & Rubber",
    "Diversified Retail": "Retail Value",
    "Edible Oil": "Edible Oils",
    "Furniture Home Furnishing": "Home Furnishings",
    "Houseware": "Home Furnishings",
    "Integrated Power Utilities": "Power Utilities",
    "Power - Transmission": "Power Utilities",
    "Power Distribution": "Power Utilities",
    "Power Trading": "Power Utilities",
    "Water Supply & Management": "Power Utilities",
    "Waste Management": "Power Utilities",
    "Paper & Paper Products": "Paper",
    "Plywood Boards/ Laminates": "Plywood & Laminates",
    "Trading & Distributors": "Trading & Distribution",
    "Unclassified": "Special Situations",
    "Consumer Electronics": "Consumer Electronics",
    "Plastic Products - Consumer": "Consumer Electronics",
    "Household Appliances": "Home Appliances",
    "Paints": "Paints",
    "Software Products": "Software Products",
    "IT Enabled Services": "IT Enabled Services",
    "Business Process Outsourcing (BPO)/ Knowledge Process Outsourcing (KPO)": "IT Enabled Services",
    "Computers Hardware & Equipments": "Hardware & Peripherals",
    "Dairy Products": "Dairy",
    "Microfinance Institutions": "NBFCs",
    "Heavy Electrical Equipment": "Capital Goods Electrical",
    "Other Electrical Equipment": "Capital Goods Electrical",
    "Industrial Products": "Capital Goods Industrial",
    "Other Industrial Products": "Capital Goods Industrial",
    "Iron & Steel Products": "Steel & Steel Products",
    "Iron & Steel": "Steel & Steel Products",
    "Investment Company": "Investment Holdings",
    "Holding Company": "Investment Holdings",
    "Diversified": "Investment Holdings",
    "General Insurance": "Insurance",
    "Life Insurance": "Insurance",
    "Insurance Distributors": "Insurance",
    "Financial Products Distributor": "Insurance",
    "Media & Entertainment": "Media & Broadcasting",
    "TV Broadcasting & Software Production": "Media & Broadcasting",
    "Advertising & Media Agencies": "Media & Broadcasting",
    "Digital Entertainment": "Media & Broadcasting",
    "Film Production Distribution & Exhibition": "Media & Broadcasting",
    "Print Media": "Media & Broadcasting",
    "Printing & Publication": "Media & Broadcasting",
    "Telecom - Infrastructure": "Telecom Infra & Equipment",
    "Telecom - Equipment & Accessories": "Telecom Infra & Equipment",
    "Commercial Vehicles": "Auto CV & Tractors",
    "Tractors": "Auto CV & Tractors",
    "Construction Vehicles": "Auto CV & Tractors",
    "Oil Exploration & Production": "Oil Upstream & Services",
    "Oil Equipment & Services": "Oil Upstream & Services",
    "Offshore Support Solution Drilling": "Oil Upstream & Services",
    "Oil Storage & Transportation": "Oil Upstream & Services",
    "Medical Equipment & Supplies": "Medical Devices",
    "Aluminium": "Non-Ferrous Metals",
    "Copper": "Non-Ferrous Metals",
    "Zinc": "Non-Ferrous Metals",
    "Aluminium Copper & Zinc Products": "Non-Ferrous Metals",
    "Glass - Consumer": "Building Materials",
    "Glass - Industrial": "Building Materials",
    "Granites & Marbles": "Building Materials",
    "Sanitary Ware": "Building Materials",
    "Ceramics": "Building Materials",
    "Other Construction Materials": "Building Materials",
    "Dyes And Pigments": "Specialty Chemicals",
    "Industrial Gases": "Specialty Chemicals",
    "Explosives": "Specialty Chemicals",
    "Carbon Black": "Commodity Chemicals",
    "Petrochemicals": "Commodity Chemicals",
    "Shipping": "Shipping & Ports",
    "Ship Building & Allied Services": "Shipping & Ports",
    "Port & Port services": "Shipping & Ports",
    "Dredging": "Shipping & Ports",
    "Electrodes & Refractories": "Electrodes & Refractories",
    "Coal": "Metals & Mining Others",
    "Diversified Metals": "Metals & Mining Others",
    "Ferro & Silica Manganese": "Metals & Mining Others",
    "Trading - Metals": "Metals & Mining Others",
    "Trading - Minerals": "Metals & Mining Others",
    "Household Products": "Personal Care",
    "Personal Care": "Personal Care",
    "Leather And Leather Products": "Textiles",
    "Rubber": "Tyres & Rubber",
    "Footwear": "Footwear",
    "Other Consumer Services": "Consumer Services",
    "Education": "Consumer Services",
    "E-Learning": "Consumer Services",
}

SECTOR_FALLBACK_GROUP_MAP: dict[str, str] = {
    "Financial Services": "Financial Institutions",
    "Capital Goods": "Capital Goods Industrial",
    "Healthcare": "Hospitals",
    "Chemicals": "Specialty Chemicals",
    "Construction": "EPC & Construction",
    "Construction Materials": "Building Materials",
    "Consumer Services": "Consumer Services",
    "Consumer Durables": "Consumer Electronics",
    "Fast Moving Consumer Goods": "FMCG Foods",
    "Information Technology": "IT Services",
    "Oil Gas & Consumable Fuels": "OMCs & Refining",
    "Metals & Mining": "Metals & Mining Others",
    "Power": "Power Utilities",
    "Services": "Business Services",
    "Telecommunication": "Telecom Services",
    "Textiles": "Textiles",
    "Media Entertainment & Publication": "Media & Broadcasting",
    "Realty": "Realty Developers",
    "Forest Materials": "Paper",
    "Utilities": "Power Utilities",
    "Automobile and Auto Components": "Auto Ancillaries",
    "Unclassified": "Special Situations",
}

INDIA_GROUP_NAME_ALIASES: dict[str, str] = {
    "Other Banks": "Small Finance Banks",
    "Special Situations": "Special Situations & SME Leaders",
    "Trading & Distribution": "Trading Houses & Distribution",
    "Consumer Services": "Education, Staffing & Retail Services",
    "Metals & Mining Others": "Mining, Coal & Alloys",
    "Power Utilities": "Power Utilities & Grids",
    "Retail Value": "Value Retail",
    "Auto 2W": "Two-Wheeler OEMs",
    "Auto PV": "Passenger Vehicle OEMs",
    "Auto CV & Tractors": "CVs, Tractors & CE",
}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or "misc-group"


def _safe_mean(values: Iterable[float]) -> float:
    numbers = [float(value) for value in values]
    return sum(numbers) / len(numbers) if numbers else 0.0


def _winsorized_mean(values: Iterable[float], lower_quantile: float = 0.1, upper_quantile: float = 0.9) -> float:
    numbers = sorted(float(value) for value in values)
    if not numbers:
        return 0.0
    if len(numbers) < 4:
        return _safe_mean(numbers)

    last_index = len(numbers) - 1
    lower_index = max(0, min(last_index, int(round(last_index * lower_quantile))))
    upper_index = max(0, min(last_index, int(round(last_index * upper_quantile))))
    lower_value = numbers[lower_index]
    upper_value = numbers[upper_index]
    clipped = [min(max(value, lower_value), upper_value) for value in numbers]
    return _safe_mean(clipped)


def _safe_pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def _avg_traded_value_50d_cr(snapshot: StockSnapshot) -> float:
    average_volume = snapshot.avg_volume_50d or snapshot.avg_volume_20d or 0
    if average_volume <= 0 or snapshot.last_price <= 0:
        return 0.0
    return round((average_volume * snapshot.last_price) / 10_000_000, 2)


def _finalize_group_name(group_name: str, market_key: str) -> str:
    if market_key == "india":
        return INDIA_GROUP_NAME_ALIASES.get(group_name, group_name)
    return group_name


def _friendly_group_name(raw_industry: str, sector: str, market_key: str, raw_counts: Counter[str]) -> str:
    normalized_raw = str(raw_industry or "Unclassified").strip() or "Unclassified"
    normalized_sector = str(sector or "Unclassified").strip() or "Unclassified"

    if market_key == "india":
        mapped = INDIA_DIRECT_GROUP_MAP.get(normalized_raw)
        if mapped:
            return _finalize_group_name(mapped, market_key)
        if raw_counts.get(normalized_raw, 0) >= GROUP_KEEP_THRESHOLD:
            return _finalize_group_name(normalized_raw, market_key)
        return _finalize_group_name(SECTOR_FALLBACK_GROUP_MAP.get(normalized_sector, normalized_raw), market_key)

    if raw_counts.get(normalized_raw, 0) >= GROUP_KEEP_THRESHOLD and normalized_raw.lower() != "unclassified":
        return normalized_raw
    if normalized_sector.lower() != "unclassified":
        return f"{normalized_sector} Others"
    return "Miscellaneous"


def _majority_label(values: Iterable[str], fallback: str = "Unclassified") -> str:
    labels = [str(value or "").strip() for value in values if str(value or "").strip()]
    if not labels:
        return fallback
    return Counter(labels).most_common(1)[0][0]


def _rank_metric_percentiles(items: list[dict[str, object]], metric_key: str) -> dict[str, float]:
    if not items:
        return {}
    ordered = sorted(
        ((float(item.get(metric_key, 0.0) or 0.0), str(item.get("group_id") or "")) for item in items),
        key=lambda value: (value[0], value[1]),
    )
    if len(ordered) == 1:
        return {ordered[0][1]: 100.0}
    total = len(ordered) - 1
    return {
        group_id: round((index / total) * 100, 2)
        for index, (_, group_id) in enumerate(ordered)
    }


def _resolve_strength_bucket(rank: int) -> str:
    if rank <= 10:
        return "Top 10"
    if rank <= 40:
        return "Top 40"
    if rank <= 60:
        return "Mid"
    return "Weak"


def _resolve_trend_label(score_change_1w: float | None, rank_change_1w: int | None) -> str:
    if score_change_1w is None or rank_change_1w is None:
        return "Stable"
    if score_change_1w >= 1.5 or rank_change_1w >= 3:
        return "Improving"
    if score_change_1w <= -1.5 or rank_change_1w <= -3:
        return "Weakening"
    return "Stable"


def _snapshot_is_eligible(snapshot: StockSnapshot, market_key: str) -> bool:
    allowed_exchanges = {"NSE", "BSE"} if market_key == "india" else {"NYSE", "NASDAQ"}
    return (
        snapshot.exchange in allowed_exchanges
        and snapshot.market_cap_crore > GROUP_MIN_MARKET_CAP_CR
        and _avg_traded_value_50d_cr(snapshot) >= GROUP_MIN_AVG_DAILY_VALUE_CR
        and snapshot.last_price > 0
    )


def _benchmark_return_summary(benchmark_snapshots: list[StockSnapshot]) -> dict[str, float]:
    if not benchmark_snapshots:
        return {"return_1m": 0.0, "return_3m": 0.0, "return_6m": 0.0}
    return {
        "return_1m": round(_winsorized_mean(snapshot.stock_return_20d for snapshot in benchmark_snapshots), 2),
        "return_3m": round(_winsorized_mean(snapshot.stock_return_60d for snapshot in benchmark_snapshots), 2),
        "return_6m": round(_winsorized_mean(snapshot.stock_return_126d for snapshot in benchmark_snapshots), 2),
    }


def _group_description(group_name: str, market_key: str) -> str:
    market_label = "Indian" if market_key == "india" else "US"
    return f"{market_label} listed {group_name.lower()} stocks passing the working liquidity and market-cap filter."


def _build_group_payload(
    snapshots: list[StockSnapshot],
    benchmark_snapshots: list[StockSnapshot],
    market_key: str,
) -> tuple[list[dict[str, object]], list[IndustryGroupStockItem], list[IndustryGroupMasterItem]]:
    eligible_snapshots = [snapshot for snapshot in snapshots if _snapshot_is_eligible(snapshot, market_key)]
    raw_counts: Counter[str] = Counter(str(snapshot.sub_sector or "Unclassified") for snapshot in eligible_snapshots)
    benchmark_returns = _benchmark_return_summary(benchmark_snapshots)

    stock_rows: list[IndustryGroupStockItem] = []
    grouped_snapshots: dict[str, list[StockSnapshot]] = defaultdict(list)
    grouped_parent_sectors: dict[str, list[str]] = defaultdict(list)

    for snapshot in eligible_snapshots:
        raw_industry = str(snapshot.sub_sector or "Unclassified") or "Unclassified"
        group_name = _friendly_group_name(raw_industry, snapshot.sector, market_key, raw_counts)
        group_id = _slugify(group_name)
        grouped_snapshots[group_id].append(snapshot)
        grouped_parent_sectors[group_id].append(snapshot.sector or "Unclassified")
        stock_rows.append(
            IndustryGroupStockItem(
                symbol=snapshot.symbol,
                company_name=snapshot.name,
                exchange=snapshot.exchange,
                market_cap_cr=round(snapshot.market_cap_crore, 2),
                avg_traded_value_50d_cr=_avg_traded_value_50d_cr(snapshot),
                sector=snapshot.sector,
                raw_industry=raw_industry,
                final_group_id=group_id,
                final_group_name=group_name,
                last_price=round(snapshot.last_price, 2),
                change_pct=round(snapshot.change_pct, 2),
                return_1m=round(snapshot.stock_return_20d, 2),
                return_3m=round(snapshot.stock_return_60d, 2),
                return_6m=round(snapshot.stock_return_126d, 2),
                return_1y=round(snapshot.stock_return_12m, 2),
                rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
            )
        )

    group_rows: list[dict[str, object]] = []
    master_rows: list[IndustryGroupMasterItem] = []
    stock_rows_by_group: dict[str, list[IndustryGroupStockItem]] = defaultdict(list)
    for stock_row in stock_rows:
        stock_rows_by_group[stock_row.final_group_id].append(stock_row)

    for group_id, members in grouped_snapshots.items():
        group_name = stock_rows_by_group[group_id][0].final_group_name
        parent_sector = _majority_label(grouped_parent_sectors[group_id])
        return_1m_values = [snapshot.stock_return_20d for snapshot in members]
        return_3m_values = [snapshot.stock_return_60d for snapshot in members]
        return_6m_values = [snapshot.stock_return_126d for snapshot in members]
        return_1m = round(_winsorized_mean(return_1m_values), 2)
        return_3m = round(_winsorized_mean(return_3m_values), 2)
        return_6m = round(_winsorized_mean(return_6m_values), 2)
        median_return_1m = round(float(median(return_1m_values)), 2) if return_1m_values else 0.0
        median_return_3m = round(float(median(return_3m_values)), 2) if return_3m_values else 0.0
        median_return_6m = round(float(median(return_6m_values)), 2) if return_6m_values else 0.0
        relative_return_1m = round(return_1m - benchmark_returns["return_1m"], 2)
        relative_return_3m = round(return_3m - benchmark_returns["return_3m"], 2)
        relative_return_6m = round(return_6m - benchmark_returns["return_6m"], 2)
        median_relative_1m = median_return_1m - benchmark_returns["return_1m"]
        median_relative_3m = median_return_3m - benchmark_returns["return_3m"]
        median_relative_6m = median_return_6m - benchmark_returns["return_6m"]
        blended_relative_1m = round((relative_return_1m * 0.6) + (median_relative_1m * 0.4), 2)
        blended_relative_3m = round((relative_return_3m * 0.6) + (median_relative_3m * 0.4), 2)
        blended_relative_6m = round((relative_return_6m * 0.6) + (median_relative_6m * 0.4), 2)

        positive_3m = _safe_pct(sum(1 for snapshot in members if snapshot.stock_return_60d > 0), len(members))
        positive_6m = _safe_pct(sum(1 for snapshot in members if snapshot.stock_return_126d > 0), len(members))
        outperform_3m = _safe_pct(sum(1 for snapshot in members if snapshot.stock_return_60d > benchmark_returns["return_3m"]), len(members))
        outperform_6m = _safe_pct(sum(1 for snapshot in members if snapshot.stock_return_126d > benchmark_returns["return_6m"]), len(members))
        above_50dma = _safe_pct(
            sum(1 for snapshot in members if (snapshot.sma50 or snapshot.ema50 or 0) > 0 and snapshot.last_price > (snapshot.sma50 or snapshot.ema50 or 0)),
            len(members),
        )
        above_200dma = _safe_pct(
            sum(1 for snapshot in members if (snapshot.sma200 or 0) > 0 and snapshot.last_price > (snapshot.sma200 or 0)),
            len(members),
        )
        breadth_score = round(_safe_mean([positive_3m, positive_6m, outperform_3m, outperform_6m]), 2)
        trend_health_score = round((above_50dma * 0.6) + (above_200dma * 0.4), 2)

        sorted_members = sorted(
            members,
            key=lambda snapshot: (
                snapshot.rs_rating if snapshot.rs_eligible else -1,
                snapshot.stock_return_126d,
                snapshot.stock_return_60d,
                snapshot.change_pct,
            ),
            reverse=True,
        )
        top_constituents = [
            IndustryGroupTopStock(
                symbol=snapshot.symbol,
                company_name=snapshot.name,
                rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
                return_1m=round(snapshot.stock_return_20d, 2),
                return_3m=round(snapshot.stock_return_60d, 2),
                return_6m=round(snapshot.stock_return_126d, 2),
                relative_return_3m=round(snapshot.stock_return_60d - benchmark_returns["return_3m"], 2),
                relative_return_6m=round(snapshot.stock_return_126d - benchmark_returns["return_6m"], 2),
            )
            for snapshot in sorted_members[:5]
        ]
        laggards = sorted(
            members,
            key=lambda snapshot: (
                snapshot.rs_rating if snapshot.rs_eligible else -1,
                snapshot.stock_return_126d,
                snapshot.stock_return_60d,
            ),
        )
        concentration_penalty = round(
            min(
                12.0,
                max(0.0, (sorted_members[0].stock_return_126d if sorted_members else 0.0) - median_return_6m) * 0.08
                + max(0, 5 - len(members)) * 1.75,
            ),
            2,
        )

        group_rows.append(
            {
                "group_id": group_id,
                "group_name": group_name,
                "parent_sector": parent_sector,
                "description": _group_description(group_name, market_key),
                "stock_count": len(members),
                "return_1m": return_1m,
                "return_3m": return_3m,
                "return_6m": return_6m,
                "relative_return_1m": relative_return_1m,
                "relative_return_3m": relative_return_3m,
                "relative_return_6m": relative_return_6m,
                "median_return_1m": median_return_1m,
                "median_return_3m": median_return_3m,
                "median_return_6m": median_return_6m,
                "pct_above_50dma": above_50dma,
                "pct_above_200dma": above_200dma,
                "pct_outperform_benchmark_3m": outperform_3m,
                "pct_outperform_benchmark_6m": outperform_6m,
                "breadth_score": breadth_score,
                "trend_health_score": trend_health_score,
                "blended_relative_1m": blended_relative_1m,
                "blended_relative_3m": blended_relative_3m,
                "blended_relative_6m": blended_relative_6m,
                "leaders": [snapshot.symbol for snapshot in sorted_members[:3]],
                "laggards": [snapshot.symbol for snapshot in laggards[:3]],
                "top_constituents": top_constituents,
                "symbols": [snapshot.symbol for snapshot in sorted_members],
                "concentration_penalty": concentration_penalty,
            }
        )

        master_rows.append(
            IndustryGroupMasterItem(
                group_id=group_id,
                group_name=group_name,
                parent_sector=parent_sector,
                description=_group_description(group_name, market_key),
                stock_count=len(members),
                symbols=sorted(snapshot.symbol for snapshot in members),
            )
        )

    percentile_1m = _rank_metric_percentiles(group_rows, "blended_relative_1m")
    percentile_3m = _rank_metric_percentiles(group_rows, "blended_relative_3m")
    percentile_6m = _rank_metric_percentiles(group_rows, "blended_relative_6m")
    for row in group_rows:
        group_id = str(row["group_id"])
        pre_penalty_score = (
            (percentile_6m.get(group_id, 0.0) * 0.40)
            + (percentile_3m.get(group_id, 0.0) * 0.25)
            + (percentile_1m.get(group_id, 0.0) * 0.20)
            + (float(row["breadth_score"]) * 0.10)
            + (float(row["trend_health_score"]) * 0.05)
        )
        row["score"] = round(max(0.0, min(100.0, pre_penalty_score - float(row["concentration_penalty"]))), 2)

    group_rows.sort(key=lambda row: (-float(row["score"]), str(row["group_name"])))
    stock_rows.sort(key=lambda row: (row.final_group_name, row.symbol))
    master_rows.sort(key=lambda row: row.group_name)
    return group_rows, stock_rows, master_rows


def build_industry_groups_response(
    snapshots: list[StockSnapshot],
    benchmark_snapshots: list[StockSnapshot],
    previous_snapshots: list[StockSnapshot],
    previous_benchmark_snapshots: list[StockSnapshot],
    *,
    generated_at: datetime,
    benchmark_label: str,
    market_key: str,
) -> IndustryGroupsResponse:
    group_rows, stock_rows, master_rows = _build_group_payload(snapshots, benchmark_snapshots, market_key)
    previous_rows, _, _ = _build_group_payload(previous_snapshots, previous_benchmark_snapshots, market_key)
    previous_rank_map = {
        str(row["group_id"]): {"rank": index + 1, "score": float(row.get("score", 0.0) or 0.0)}
        for index, row in enumerate(sorted(previous_rows, key=lambda row: (-float(row["score"]), str(row["group_name"]))))
    }

    ranked_groups: list[IndustryGroupRankItem] = []
    for index, row in enumerate(group_rows, start=1):
        previous = previous_rank_map.get(str(row["group_id"]))
        rank_change_1w = (int(previous["rank"]) - index) if previous is not None else None
        score_change_1w = round(float(row["score"]) - float(previous["score"]), 2) if previous is not None else None
        ranked_groups.append(
            IndustryGroupRankItem(
                rank=index,
                rank_label=f"#{index}",
                rank_change_1w=rank_change_1w,
                score_change_1w=score_change_1w,
                strength_bucket=_resolve_strength_bucket(index),
                trend_label=_resolve_trend_label(score_change_1w, rank_change_1w),
                group_id=str(row["group_id"]),
                group_name=str(row["group_name"]),
                parent_sector=str(row["parent_sector"]),
                description=str(row["description"]),
                stock_count=int(row["stock_count"]),
                score=float(row["score"]),
                return_1m=float(row["return_1m"]),
                return_3m=float(row["return_3m"]),
                return_6m=float(row["return_6m"]),
                relative_return_1m=float(row["relative_return_1m"]),
                relative_return_3m=float(row["relative_return_3m"]),
                relative_return_6m=float(row["relative_return_6m"]),
                median_return_1m=float(row["median_return_1m"]),
                median_return_3m=float(row["median_return_3m"]),
                median_return_6m=float(row["median_return_6m"]),
                pct_above_50dma=float(row["pct_above_50dma"]),
                pct_above_200dma=float(row["pct_above_200dma"]),
                pct_outperform_benchmark_3m=float(row["pct_outperform_benchmark_3m"]),
                pct_outperform_benchmark_6m=float(row["pct_outperform_benchmark_6m"]),
                breadth_score=float(row["breadth_score"]),
                trend_health_score=float(row["trend_health_score"]),
                leaders=list(row["leaders"]),
                laggards=list(row["laggards"]),
                top_constituents=list(row["top_constituents"]),
                symbols=list(row["symbols"]),
            )
        )

    as_of_date = generated_at.astimezone(timezone.utc).date().isoformat()
    return IndustryGroupsResponse(
        generated_at=generated_at,
        as_of_date=as_of_date,
        benchmark=benchmark_label,
        filters=IndustryGroupFilters(
            min_market_cap_cr=GROUP_MIN_MARKET_CAP_CR,
            min_avg_daily_value_cr=GROUP_MIN_AVG_DAILY_VALUE_CR,
        ),
        total_groups=len(ranked_groups),
        groups=ranked_groups,
        master=master_rows,
        stocks=stock_rows,
    )


def write_industry_group_files(
    response: IndustryGroupsResponse,
    *,
    groups_path: Path,
    ranks_path: Path,
    stocks_path: Path,
) -> None:
    groups_path.parent.mkdir(parents=True, exist_ok=True)

    groups_payload = {
        "asOfDate": response.as_of_date,
        "benchmark": response.benchmark,
        "filters": {
            "minMarketCapCr": response.filters.min_market_cap_cr,
            "minAvgDailyValueCr": response.filters.min_avg_daily_value_cr,
        },
        "master": [
            {
                "groupId": item.group_id,
                "groupName": item.group_name,
                "parentSector": item.parent_sector,
                "description": item.description,
                "stockCount": item.stock_count,
                "symbols": item.symbols,
            }
            for item in response.master
        ],
        "groups": [
            {
                "rank": item.rank,
                "rankLabel": item.rank_label,
                "rankChange1w": item.rank_change_1w,
                "scoreChange1w": item.score_change_1w,
                "strengthBucket": item.strength_bucket,
                "trendLabel": item.trend_label,
                "groupId": item.group_id,
                "groupName": item.group_name,
                "parentSector": item.parent_sector,
                "description": item.description,
                "score": item.score,
                "returns": {"1m": item.return_1m, "3m": item.return_3m, "6m": item.return_6m},
                "relativeReturns": {
                    "1m": item.relative_return_1m,
                    "3m": item.relative_return_3m,
                    "6m": item.relative_return_6m,
                },
                "breadth": {
                    "above50dma": item.pct_above_50dma,
                    "above200dma": item.pct_above_200dma,
                    "positive3m": item.breadth_score,
                    "positive6m": item.pct_outperform_benchmark_6m,
                },
                "stockCount": item.stock_count,
                "leaders": item.leaders,
                "laggards": item.laggards,
                "symbols": item.symbols,
            }
            for item in response.groups
        ],
    }
    rank_payload = [
        {
            "rank": item.rank,
            "groupId": item.group_id,
            "groupName": item.group_name,
            "score": item.score,
            "return1m": item.return_1m,
            "return3m": item.return_3m,
            "return6m": item.return_6m,
            "relativeReturn1m": item.relative_return_1m,
            "relativeReturn3m": item.relative_return_3m,
            "relativeReturn6m": item.relative_return_6m,
            "above50dma": item.pct_above_50dma,
            "above200dma": item.pct_above_200dma,
            "breadthScore": item.breadth_score,
            "leaders": item.leaders,
            "topConstituents": [top_stock.model_dump(mode="json") for top_stock in item.top_constituents],
            "strengthBucket": item.strength_bucket,
            "trendLabel": item.trend_label,
            "scoreChange1w": item.score_change_1w,
            "rankChange1w": item.rank_change_1w,
        }
        for item in response.groups
    ]
    stock_payload = [
        {
            "symbol": item.symbol,
            "companyName": item.company_name,
            "exchange": item.exchange,
            "marketCapCr": item.market_cap_cr,
            "avgTradedValue50dCr": item.avg_traded_value_50d_cr,
            "sector": item.sector,
            "rawIndustry": item.raw_industry,
            "finalGroupId": item.final_group_id,
            "finalGroupName": item.final_group_name,
            "lastPrice": item.last_price,
            "changePct": item.change_pct,
            "return1m": item.return_1m,
            "return3m": item.return_3m,
            "return6m": item.return_6m,
            "return1y": item.return_1y,
            "rsRating": item.rs_rating,
        }
        for item in response.stocks
    ]

    groups_path.write_text(json.dumps(groups_payload, indent=2), encoding="utf-8")
    ranks_path.write_text(json.dumps(rank_payload, indent=2), encoding="utf-8")
    stocks_path.write_text(json.dumps(stock_payload, indent=2), encoding="utf-8")