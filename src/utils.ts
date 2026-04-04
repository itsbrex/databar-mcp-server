/**
 * Utility functions for enrichment categorization and formatting
 */

import {
  Enrichment,
  ExporterInfo,
  ExporterDetails,
  InstalledExporter,
  Table,
  Column,
  TableEnrichment,
  CreateRowsResponse,
  PatchRowsResponse,
  UpsertRowsResponse
} from './types.js';

/**
 * Filter enrichments by category name
 */
export function filterByCategory(
  enrichments: Enrichment[],
  categoryName: string
): Enrichment[] {
  const lower = categoryName.toLowerCase();
  return enrichments.filter(e =>
    e.category?.some(c => c.name.toLowerCase().includes(lower))
  );
}

/**
 * Format enrichment for display
 */
export function formatEnrichmentForDisplay(enrichment: Enrichment): string {
  const categoryNames = enrichment.category?.map(c => c.name).join(', ') || 'Uncategorized';

  const lines = [
    `ID: ${enrichment.id}`,
    `Name: ${enrichment.name}`,
    `Category: ${categoryNames}`,
    `Description: ${enrichment.description}`,
    `Data Source: ${enrichment.data_source}`,
    `Price: ${enrichment.price} credits`,
    enrichment.rank ? `Rank: ${enrichment.rank}` : '',
    enrichment.params ? `Required Parameters: ${enrichment.params.filter(p => p.is_required).map(p => p.name).join(', ')}` : ''
  ];

  const choiceParams = enrichment.params?.filter(p =>
    p.choices && (p.type_field === 'select' || p.type_field === 'mselect')
  );
  if (choiceParams && choiceParams.length > 0) {
    const parts = choiceParams.map(p => {
      if (p.choices?.mode === 'inline' && p.choices.items) {
        return `${p.name}: ${p.choices.items.map(i => i.id).join(', ')}`;
      }
      return `${p.name}: use get_param_choices to browse`;
    });
    lines.push(`Select Parameters: ${parts.join('; ')}`);
  }

  return lines.filter(Boolean).join('\n');
}

/**
 * Format waterfall for display
 */
export function formatWaterfallForDisplay(waterfall: any): string {
  return [
    `Identifier: ${waterfall.identifier}`,
    `Name: ${waterfall.name}`,
    `Description: ${waterfall.description}`,
    `Required Parameters: ${waterfall.input_params.filter((p: any) => p.required).map((p: any) => p.name).join(', ')}`,
    `Available Providers: ${waterfall.available_enrichments.length}`
  ].join('\n');
}

/**
 * Format generic results for display
 */
export function formatResults(data: any): string {
  if (!data) return 'No data returned';
  if (typeof data === 'string') return data;
  return JSON.stringify(data, null, 2);
}

/**
 * Extract required parameter names from an enrichment
 */
export function getRequiredParams(enrichment: Enrichment): string[] {
  if (!enrichment.params) return [];
  return enrichment.params
    .filter(p => p.is_required)
    .map(p => p.name);
}

/**
 * Validate parameters against enrichment requirements
 */
export function validateParams(
  enrichment: Enrichment,
  providedParams: Record<string, any>
): { valid: boolean; missing: string[]; errors: string[] } {
  const errors: string[] = [];
  const missing: string[] = [];

  if (!enrichment.params) {
    return { valid: true, missing: [], errors: [] };
  }

  for (const param of enrichment.params) {
    if (param.is_required && !providedParams[param.name]) {
      missing.push(param.name);
      errors.push(`Missing required parameter: ${param.name}`);
    }

    const value = providedParams[param.name];
    if (value != null && param.choices?.mode === 'inline' && param.choices.items) {
      const validIds = param.choices.items.map(i => i.id);
      const valuesToCheck = Array.isArray(value) ? value : [value];
      for (const v of valuesToCheck) {
        if (!validIds.includes(String(v))) {
          errors.push(
            `Invalid value "${v}" for parameter "${param.name}". Valid options: ${validIds.join(', ')}`
          );
        }
      }
    }
  }

  return { valid: errors.length === 0, missing, errors };
}

/**
 * Format table for display
 */
export function formatTableForDisplay(table: Table): string {
  return [
    `UUID: ${table.identifier}`,
    `Name: ${table.name}`,
    `URL: ${table.table_url}`,
    `Created: ${table.created_at}`,
    `Updated: ${table.updated_at}`
  ].join('\n');
}

/**
 * Format column for display
 */
export function formatColumnForDisplay(column: Column): string {
  const filterName = column.additional_intenal_name ? ` | filter_name: ${column.additional_intenal_name}` : '';
  return `${column.name} (${column.type_of_value}) [${column.identifier}] | internal_name: ${column.internal_name}${filterName}`;
}

/**
 * Format table enrichment for display
 */
export function formatTableEnrichmentForDisplay(enrichment: TableEnrichment): string {
  return `ID: ${enrichment.id} — ${enrichment.name}`;
}

/**
 * Search exporters by query string.
 */
export function searchExporters(
  exporters: ExporterInfo[],
  query: string
): ExporterInfo[] {
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  if (words.length === 0) return exporters;

  return exporters.filter(exporter => {
    const fields = [
      exporter.name,
      exporter.description,
    ].map(f => (f || '').toLowerCase());

    return words.every(word => fields.some(f => f.includes(word)));
  });
}

/**
 * Format exporter for display
 */
export function formatExporterForDisplay(exporter: ExporterDetails | ExporterInfo): string {
  const lines = [
    `ID: ${exporter.id}`,
    `Name: ${exporter.name}`,
    `Description: ${exporter.description}`,
  ];

  const details = exporter as ExporterDetails;
  if (details.params) {
    const required = details.params.filter(p => p.is_required).map(p => p.name);
    if (required.length > 0) {
      lines.push(`Required Parameters: ${required.join(', ')}`);
    }
  }

  if (details.authorization) {
    lines.push(`Authorization Required: ${details.authorization.required ? 'Yes' : 'No'}`);
    if (details.authorization.connections.length > 0) {
      const conns = details.authorization.connections.map(
        c => `  - ID: ${c.id}, Name: ${c.name}, Type: ${c.type}`
      );
      lines.push(`Available Connections:\n${conns.join('\n')}`);
    } else if (details.authorization.required) {
      lines.push('Available Connections: None (connect via Databar UI first)');
    }
  }

  return lines.filter(Boolean).join('\n');
}

/**
 * Format table exporter for display
 */
export function formatTableExporterForDisplay(exporter: InstalledExporter): string {
  return `ID: ${exporter.id} — ${exporter.name}`;
}

/**
 * Format create-rows response for display
 */
export function formatCreateRowsResponse(response: CreateRowsResponse): string {
  const lines: string[] = [];
  lines.push(`Created: ${response.created?.length ?? 0} row(s)`);
  if (response.created?.length) {
    for (const item of response.created) {
      lines.push(`  - ${item.rowId}`);
    }
  }
  if (response.errors?.length) {
    lines.push(`Errors: ${response.errors.length}`);
    for (const err of response.errors) {
      lines.push(`  - Index ${err.index}: ${err.error.code} — ${err.error.message}`);
    }
  }
  return lines.join('\n');
}

/**
 * Format patch-rows response for display
 */
export function formatPatchRowsResponse(response: PatchRowsResponse): string {
  const ok = response.results?.filter(r => r.ok).length ?? 0;
  const failed = response.results?.filter(r => !r.ok).length ?? 0;
  const lines: string[] = [`Updated: ${ok}, Failed: ${failed}`];
  if (failed > 0) {
    for (const item of response.results.filter(r => !r.ok)) {
      lines.push(`  - Row ${item.id}: ${item.error?.code} — ${item.error?.message}`);
    }
  }
  return lines.join('\n');
}

/**
 * Format upsert-rows response for display
 */
export function formatUpsertRowsResponse(response: UpsertRowsResponse): string {
  const created = response.results?.filter(r => r.action === 'created').length ?? 0;
  const updated = response.results?.filter(r => r.action === 'updated').length ?? 0;
  const failed = response.results?.filter(r => !r.ok).length ?? 0;
  const lines: string[] = [`Created: ${created}, Updated: ${updated}, Failed: ${failed}`];
  if (failed > 0) {
    for (const item of response.results.filter(r => !r.ok)) {
      lines.push(`  - Index ${item.index}: ${item.error?.code} — ${item.error?.message}`);
    }
  }
  return lines.join('\n');
}
