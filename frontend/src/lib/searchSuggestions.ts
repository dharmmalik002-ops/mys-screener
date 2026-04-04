type SearchableSymbol = {
  symbol: string;
  name: string;
};

export function buildSymbolSuggestions<T extends SearchableSymbol>(items: T[], query: string, limit: number): T[] {
  if (!items.length) {
    return [];
  }

  const maxResults = Math.max(1, limit);
  const rawQuery = query.trim();
  const normalizedQuery = (rawQuery.toLowerCase().startsWith("group:") ? rawQuery.slice(6) : rawQuery).trim().toUpperCase();

  if (!normalizedQuery) {
    return items.slice(0, Math.min(maxResults, 120));
  }

  const exact: T[] = [];
  const symbolPrefix: T[] = [];
  const namePrefix: T[] = [];
  const contains: T[] = [];

  for (const item of items) {
    const symbol = String(item.symbol || "").toUpperCase();
    const name = String(item.name || "").toUpperCase();

    if (!symbol) {
      continue;
    }

    if (symbol === normalizedQuery) {
      exact.push(item);
    } else if (symbol.startsWith(normalizedQuery)) {
      symbolPrefix.push(item);
    } else if (name.startsWith(normalizedQuery)) {
      namePrefix.push(item);
    } else if (symbol.includes(normalizedQuery) || name.includes(normalizedQuery)) {
      contains.push(item);
    }

    if ((exact.length + symbolPrefix.length + namePrefix.length + contains.length) >= maxResults * 3) {
      break;
    }
  }

  return [...exact, ...symbolPrefix, ...namePrefix, ...contains].slice(0, maxResults);
}