/**
 * MiniSearch-based full-text search index for enrichments.
 *
 * Provides fuzzy matching, prefix search, BM25 scoring, and per-field
 * boosting on the in-memory enrichments cache.
 */

import MiniSearch from 'minisearch';
import { Enrichment } from './types.js';

interface IndexedDoc {
  id: number;
  name: string;
  search_keywords: string;
  categories: string;
  description: string;
  data_source: string;
}

function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/[-_,]/g, ' ')
    .split(/\s+/)
    .filter(Boolean);
}

const SEARCH_OPTIONS = {
  boost: {
    name: 10,
    search_keywords: 8,
    categories: 6,
    description: 3,
    data_source: 2,
  },
  fuzzy: 0.2,
  prefix: true,
  combineWith: 'OR' as const,
};

export class EnrichmentSearchIndex {
  private index: MiniSearch<IndexedDoc>;
  private enrichmentMap: Map<number, Enrichment> = new Map();

  constructor() {
    this.index = new MiniSearch<IndexedDoc>({
      fields: ['name', 'search_keywords', 'categories', 'description', 'data_source'],
      idField: 'id',
      tokenize,
      searchOptions: SEARCH_OPTIONS,
    });
  }

  rebuild(enrichments: Enrichment[]): void {
    this.index.removeAll();
    this.enrichmentMap.clear();

    const docs: IndexedDoc[] = [];
    for (const e of enrichments) {
      this.enrichmentMap.set(e.id, e);
      docs.push({
        id: e.id,
        name: e.name ?? '',
        search_keywords: e.search_keywords ?? '',
        categories: (e.category?.map(c => c.name) ?? []).join(' '),
        description: e.description ?? '',
        data_source: e.data_source ?? '',
      });
    }

    this.index.addAll(docs);
  }

  search(query: string, limit: number = 10, categoryFilter?: string): Enrichment[] {
    if (!query.trim()) return [];

    const opts = categoryFilter
      ? {
          filter: (result: { id: any }) => {
            const e = this.enrichmentMap.get(result.id as number);
            return !!e?.category?.some(
              c => c.name.toLowerCase().includes(categoryFilter.toLowerCase())
            );
          },
        }
      : undefined;

    const results = this.index.search(query, opts);

    const enrichments: Enrichment[] = [];
    for (const result of results) {
      const e = this.enrichmentMap.get(result.id as number);
      if (e) enrichments.push(e);
    }

    return enrichments.slice(0, limit);
  }
}
