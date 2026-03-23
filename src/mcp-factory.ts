/**
 * Factory for creating configured MCP Server instances.
 * Used by both stdio (index.ts) and HTTP (http-server.ts) entry points.
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool,
} from '@modelcontextprotocol/sdk/types.js';
import { DatabarClient } from './databar-client.js';
import { Cache } from './cache.js';
import {
  searchEnrichments,
  filterByCategory,
  formatEnrichmentForDisplay,
  formatWaterfallForDisplay,
  formatResults,
  getRequiredParams,
  validateParams,
  formatTableForDisplay,
  formatColumnForDisplay,
  formatTableEnrichmentForDisplay,
  formatCreateRowsResponse,
  formatPatchRowsResponse,
  formatUpsertRowsResponse
} from './utils.js';
import {
  loadSpendingConfig,
  checkSpendingGuard,
  unsafeModeWarning,
  validateBulkArray,
  validateRowBatchSize,
  sanitizeResult,
  SpendingConfig
} from './guards.js';
import { auditLog } from './audit.js';
import { DatabarConfig, Enrichment } from './types.js';

const TOOLS: Tool[] = [
  {
    name: 'search_enrichments',
    description: 'Search and discover available data enrichments. Use this to find the right enrichment for a specific task (e.g., "linkedin profile", "email finder", "company data"). Returns a list of matching enrichments with their IDs, descriptions, required parameters, and pricing. Results are sorted by recommendation rank (best options first). BYOK providers that the user has not connected are automatically excluded.',
    inputSchema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'Search query to find enrichments (e.g., "linkedin", "email verification", "company data", "job postings", "tech stack")'
        },
        category: {
          type: 'string',
          description: 'Optional: Filter by category name (e.g., "Company Data", "Contact Finding", "Hiring Signals", "Tech Stack", "SEO", "Reviews")'
        },
        limit: {
          type: 'number',
          description: 'Maximum number of results to return (default: 10)',
          default: 10
        }
      },
      required: ['query']
    }
  },
  {
    name: 'get_enrichment_details',
    description: 'Get detailed information about a specific enrichment, including all required and optional parameters, response fields, pricing, and data source. Use this before running an enrichment to understand what parameters are needed.',
    inputSchema: {
      type: 'object',
      properties: {
        enrichment_id: {
          type: 'number',
          description: 'The ID of the enrichment to get details for'
        }
      },
      required: ['enrichment_id']
    }
  },
  {
    name: 'run_enrichment',
    description: 'Execute a data enrichment with the provided parameters. Automatically handles async execution and polling, returning final results. Results are cached for 24 hours to reduce costs. Subject to spending limits (DATABAR_MAX_COST_PER_REQUEST, DATABAR_MIN_BALANCE). For paginated enrichments, use the pages parameter to fetch multiple pages (each page is billed separately).',
    inputSchema: {
      type: 'object',
      properties: {
        enrichment_id: {
          type: 'number',
          description: 'The ID of the enrichment to run'
        },
        params: {
          type: 'object',
          description: 'Parameters required by the enrichment (e.g., {"email": "test@example.com"})',
          additionalProperties: true
        },
        skip_cache: {
          type: 'boolean',
          description: 'Skip cache and fetch fresh data (default: false)',
          default: false
        },
        pages: {
          type: 'number',
          description: 'Number of pages to fetch for paginated enrichments (default: 1, max: 100). Each page is billed separately. Use get_enrichment_details to check if pagination is supported.',
          default: 1,
          minimum: 1,
          maximum: 100
        }
      },
      required: ['enrichment_id', 'params']
    }
  },
  {
    name: 'run_bulk_enrichment',
    description: 'Execute an enrichment on multiple inputs at once. Provide an array of parameter objects. Subject to spending limits. For paginated enrichments, use the pages parameter to fetch multiple pages per record (each page per record is billed separately).',
    inputSchema: {
      type: 'object',
      properties: {
        enrichment_id: {
          type: 'number',
          description: 'The ID of the enrichment to run'
        },
        params_list: {
          type: 'array',
          items: { type: 'object', additionalProperties: true },
          description: 'Array of parameter objects, one per record'
        },
        pages: {
          type: 'number',
          description: 'Number of pages to fetch per record for paginated enrichments (default: 1, max: 100). Each page per record is billed separately.',
          default: 1,
          minimum: 1,
          maximum: 100
        }
      },
      required: ['enrichment_id', 'params_list']
    }
  },
  {
    name: 'get_param_choices',
    description: 'Get available choices for a select/mselect enrichment parameter. Supports search and pagination. Use this when get_enrichment_details shows a parameter with choices.mode = "remote". For inline choices, the values are already included in get_enrichment_details.',
    inputSchema: {
      type: 'object',
      properties: {
        enrichment_id: {
          type: 'number',
          description: 'The enrichment ID'
        },
        param_name: {
          type: 'string',
          description: 'The parameter name (slug)'
        },
        q: {
          type: 'string',
          description: 'Optional search query to filter choices by id or name'
        },
        page: {
          type: 'number',
          description: 'Page number (default: 1)',
          default: 1
        },
        limit: {
          type: 'number',
          description: 'Items per page (default: 100, max: 500)',
          default: 100
        }
      },
      required: ['enrichment_id', 'param_name']
    }
  },
  {
    name: 'search_waterfalls',
    description: 'Search available waterfall enrichments. Waterfalls try multiple data providers in sequence until one succeeds, maximizing data retrieval success rate.',
    inputSchema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'Search query (e.g., "email finder", "phone lookup")'
        }
      },
      required: ['query']
    }
  },
  {
    name: 'run_waterfall',
    description: 'Execute a waterfall enrichment that tries multiple providers until one succeeds. Subject to spending limits.',
    inputSchema: {
      type: 'object',
      properties: {
        waterfall_identifier: {
          type: 'string',
          description: 'The identifier of the waterfall to run (e.g., "email_getter")'
        },
        params: {
          type: 'object',
          description: 'Parameters required by the waterfall',
          additionalProperties: true
        },
        provider_ids: {
          type: 'array',
          items: { type: 'number' },
          description: 'Optional: Specific provider IDs to use (default: uses all in cost-optimized order)'
        },
        email_verifier: {
          type: 'number',
          description: 'Optional: Email verifier enrichment ID to verify results'
        }
      },
      required: ['waterfall_identifier', 'params']
    }
  },
  {
    name: 'run_bulk_waterfall',
    description: 'Execute a waterfall enrichment on multiple inputs at once. Subject to spending limits.',
    inputSchema: {
      type: 'object',
      properties: {
        waterfall_identifier: {
          type: 'string',
          description: 'The identifier of the waterfall to run'
        },
        params_list: {
          type: 'array',
          items: { type: 'object', additionalProperties: true },
          description: 'Array of parameter objects, one per record'
        },
        provider_ids: {
          type: 'array',
          items: { type: 'number' },
          description: 'Optional: Specific provider IDs to use'
        },
        email_verifier: {
          type: 'number',
          description: 'Optional: Email verifier enrichment ID'
        }
      },
      required: ['waterfall_identifier', 'params_list']
    }
  },
  {
    name: 'create_table',
    description: 'Create a new table in your Databar workspace. Optionally specify a name, column names, and number of empty rows. By default creates columns column1/column2/column3 and 0 rows.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Table name (default: "New empty table")' },
        columns: {
          type: 'array',
          items: { type: 'string' },
          description: 'Column names. Default: ["column1","column2","column3"]. Pass empty array [] to create a table with no columns.'
        },
        rows: { type: 'number', description: 'Number of empty rows to create (default: 0)', default: 0 }
      }
    }
  },
  {
    name: 'list_tables',
    description: 'List all tables in your Databar workspace. Returns table UUIDs, names, and timestamps.',
    inputSchema: { type: 'object', properties: {} }
  },
  {
    name: 'get_table_columns',
    description: 'Get all columns defined on a table. Returns column names, types, and identifiers.',
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' }
      },
      required: ['table_uuid']
    }
  },
  {
    name: 'get_table_rows',
    description: 'Get rows from a table with pagination and optional filtering. Returns up to 100 rows per page by default (max 500). Supports Airtable-style structured filters with 5 operators: equals, contains, not_equals, is_empty, is_not_empty. Multiple filters use AND logic.',
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' },
        page: { type: 'number', description: 'Page number (default: 1)', default: 1 },
        per_page: { type: 'number', description: 'Rows per page (default: 100, max: 500)', default: 100, maximum: 500 },
        filter: {
          type: 'object',
          description: 'Filter rows by column values (AND logic). Keys are column names, values are objects with one operator. Operators: equals, contains (case-insensitive), not_equals, is_empty (true), is_not_empty (true). Examples: {"company":{"contains":"tech"}}, {"status":{"equals":"active"}}, {"email":{"is_not_empty":true}}, {"name":{"contains":"a"},"revenue":{"equals":"5000"}}',
          additionalProperties: {
            type: 'object',
            properties: {
              equals: { type: 'string', description: 'Exact match' },
              contains: { type: 'string', description: 'Substring match (case-insensitive)' },
              not_equals: { type: 'string', description: 'Not equal to value' },
              is_empty: { type: 'boolean', description: 'True to find rows where column is empty/null' },
              is_not_empty: { type: 'boolean', description: 'True to find rows where column has a value' }
            }
          }
        }
      },
      required: ['table_uuid']
    }
  },
  {
    name: 'create_rows',
    description: 'Insert new rows into a table (max 100 per request). To add new columns to an existing table, set options.allow_new_columns to true — any column name in fields that does not exist yet will be auto-created as a text column.',
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' },
        rows: {
          type: 'array',
          items: {
            type: 'object',
            properties: { fields: { type: 'object', additionalProperties: true } },
            required: ['fields']
          },
          description: 'Array of rows to insert (max 100). Each row has a fields object keyed by column name.',
          maxItems: 100
        },
        options: {
          type: 'object',
          properties: {
            allow_new_columns: { type: 'boolean', description: 'Auto-create unknown column names as text columns (default: false). This is the only way to add columns to an existing table via the API.' },
            dedupe: {
              type: 'object',
              properties: {
                enabled: { type: 'boolean' },
                keys: { type: 'array', items: { type: 'string' } }
              }
            }
          }
        }
      },
      required: ['table_uuid', 'rows']
    }
  },
  {
    name: 'patch_rows',
    description: 'Update specific fields on existing rows by row ID (max 100 per request).',
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' },
        rows: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              id: { type: 'string', description: 'Row ID' },
              fields: { type: 'object', additionalProperties: true }
            },
            required: ['id', 'fields']
          },
          description: 'Array of patch operations (max 100)',
          maxItems: 100
        },
        overwrite: { type: 'boolean', description: 'Overwrite non-empty cells (default: true)', default: true }
      },
      required: ['table_uuid', 'rows']
    }
  },
  {
    name: 'upsert_rows',
    description: 'Insert or update rows by matching key (max 100 per request).',
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' },
        rows: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              key: { type: 'object', additionalProperties: true },
              fields: { type: 'object', additionalProperties: true }
            },
            required: ['key', 'fields']
          },
          description: 'Array of upsert operations (max 100)',
          maxItems: 100
        }
      },
      required: ['table_uuid', 'rows']
    }
  },
  {
    name: 'get_table_enrichments',
    description: 'List all enrichments configured on a table.',
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' }
      },
      required: ['table_uuid']
    }
  },
  {
    name: 'add_table_enrichment',
    description: `Add an enrichment to a table with a parameter-to-column mapping.

IMPORTANT — mapping format:
Each key is an enrichment parameter name. Each value is one of:
  • { "type": "mapping", "value": "<column-name>" }  — read value from a table column per row. Use the human-readable column name (e.g. "email"). The server accepts column names directly.
  • { "type": "simple", "value": "<static-value>" }  — pass the same hardcoded value for every row.

WORKFLOW:
1. Call get_enrichment_details to see the parameter names.
2. Call get_table_columns to see available column names.
3. Build the mapping using column names (not UUIDs).
4. The returned enrichment_id from this call is the TABLE-ENRICHMENT id — use it with run_table_enrichment (NOT the original enrichment_id).`,
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' },
        enrichment_id: { type: 'number', description: 'The enrichment ID to add (from search_enrichments or get_enrichment_details)' },
        mapping: {
          type: 'object',
          description: 'Parameter-to-column mapping. Keys = enrichment param names. Values = { type: "mapping", value: "column-name" } or { type: "simple", value: "static-value" }',
          additionalProperties: {
            type: 'object',
            properties: {
              type: { type: 'string', enum: ['mapping', 'simple'] },
              value: { type: 'string' }
            },
            required: ['type', 'value']
          }
        }
      },
      required: ['table_uuid', 'enrichment_id', 'mapping']
    }
  },
  {
    name: 'run_table_enrichment',
    description: 'Trigger an enrichment or waterfall to run on all rows in a table. Works for both enrichments (from add_table_enrichment) and waterfalls (from add_table_waterfall). Subject to spending limits.',
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' },
        enrichment_id: { type: 'string', description: 'The table enrichment/waterfall ID to run (returned by add_table_enrichment or add_table_waterfall)' }
      },
      required: ['table_uuid', 'enrichment_id']
    }
  },
  {
    name: 'add_table_waterfall',
    description: `Add a waterfall to a table. A waterfall tries multiple data providers in sequence until one returns a result.

WORKFLOW:
1. Call search_waterfalls to find the right waterfall (e.g. "email_getter", "person_getter").
2. Note the waterfall identifier, available_enrichments (provider IDs), and input_params.
3. Call get_table_columns to see available column names.
4. Build the mapping: keys are waterfall param names, values are column names.
5. The returned id is the TABLE-WATERFALL id — use it with run_table_enrichment to trigger a run.`,
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' },
        waterfall_identifier: { type: 'string', description: 'The waterfall identifier (e.g. "email_getter"). Get from search_waterfalls.' },
        enrichments: {
          type: 'array',
          items: { type: 'number' },
          description: 'List of enrichment (provider) IDs to use in the waterfall cascade. Get from search_waterfalls available_enrichments.',
          minItems: 1
        },
        mapping: {
          type: 'object',
          description: 'Maps waterfall param names to table column names. Keys = param names from waterfall input_params. Values = column names from get_table_columns.',
          additionalProperties: { type: 'string' }
        },
        email_verifier: {
          type: 'number',
          description: 'Optional enrichment ID for email verification (only for email waterfalls with is_email_verifying=true).'
        }
      },
      required: ['table_uuid', 'waterfall_identifier', 'enrichments', 'mapping']
    }
  },
  {
    name: 'get_table_waterfalls',
    description: 'List all waterfalls installed on a table. Returns waterfall IDs that can be used with run_table_enrichment.',
    inputSchema: {
      type: 'object',
      properties: {
        table_uuid: { type: 'string', description: 'The UUID of the table' }
      },
      required: ['table_uuid']
    }
  },
  {
    name: 'get_user_balance',
    description: 'Get the current user\'s credit balance and account information.',
    inputSchema: { type: 'object', properties: {} }
  }
];

/**
 * Create a fully configured MCP Server for a given API key.
 * Each call returns an independent instance with its own DatabarClient and cache.
 */
export function createMcpServer(apiKey: string): Server {
  const config: DatabarConfig = {
    apiKey,
    baseUrl: process.env.DATABAR_BASE_URL || 'https://api.databar.ai/v1',
    cacheTtlHours: parseInt(process.env.CACHE_TTL_HOURS || '24'),
    maxPollAttempts: parseInt(process.env.MAX_POLL_ATTEMPTS || '150'),
    pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS || '2000'),
  };

  const spendingConfig: SpendingConfig = loadSpendingConfig();
  const databarClient = new DatabarClient(config);
  const cache = new Cache(config.cacheTtlHours);

  let enrichmentsCache: Enrichment[] | null = null;
  let enrichmentsCacheTime: number = 0;
  const ENRICHMENTS_CACHE_TTL = 5 * 60 * 1000;

  async function getCachedEnrichments(): Promise<Enrichment[]> {
    const now = Date.now();
    if (enrichmentsCache && (now - enrichmentsCacheTime) < ENRICHMENTS_CACHE_TTL) {
      return enrichmentsCache;
    }
    enrichmentsCache = await databarClient.getAllEnrichments();
    enrichmentsCacheTime = now;
    return enrichmentsCache;
  }

  async function guardSpending(
    toolName: string,
    estimatedCost: number,
    params: Record<string, any>
  ): Promise<{ content: { type: string; text: string }[]; isError: true } | null> {
    const blocked = await checkSpendingGuard(databarClient, estimatedCost, spendingConfig);
    if (blocked) {
      auditLog({ timestamp: new Date().toISOString(), tool: toolName, params, estimatedCost, result: 'blocked', message: blocked });
      return { content: [{ type: 'text', text: blocked }], isError: true };
    }
    return null;
  }

  function safeResult(text: string): string {
    return sanitizeResult(text, spendingConfig.maxResultLength);
  }

  const server = new Server(
    { name: 'databar-mcp-server', version: '1.3.0' },
    { capabilities: { tools: {} } }
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => {
    return { tools: TOOLS };
  });

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    const ts = new Date().toISOString();

    try {
      switch (name) {

        case 'search_enrichments': {
          const { query, category, limit = 10 } = args as {
            query: string; category?: string; limit?: number;
          };
          auditLog({ timestamp: ts, tool: name, params: { query, category, limit }, result: 'success' });
          let enrichments = await getCachedEnrichments();
          if (category) enrichments = filterByCategory(enrichments, category);
          enrichments = searchEnrichments(enrichments, query).slice(0, limit);
          if (enrichments.length === 0) {
            return { content: [{ type: 'text', text: `No enrichments found matching "${query}".` }] };
          }
          return {
            content: [{ type: 'text', text: safeResult(
              `Found ${enrichments.length} enrichment(s):\n\n${enrichments.map(formatEnrichmentForDisplay).join('\n\n---\n\n')}`
            )}]
          };
        }

        case 'get_enrichment_details': {
          const { enrichment_id } = args as { enrichment_id: number };
          auditLog({ timestamp: ts, tool: name, params: { enrichment_id }, result: 'success' });
          const enrichment = await databarClient.getEnrichmentDetails(enrichment_id);
          const categoryNames = enrichment.category?.map(c => c.name).join(', ') || 'Uncategorized';
          const details: Record<string, any> = {
            id: enrichment.id, name: enrichment.name, category: categoryNames,
            description: enrichment.description, data_source: enrichment.data_source,
            price: enrichment.price, auth_method: enrichment.auth_method,
            rank: enrichment.rank || 0,
            parameters: enrichment.params?.map(p => {
              const param: Record<string, any> = {
                name: p.name, required: p.is_required, type: p.type_field, description: p.description,
              };
              if (p.choices) {
                param.choices = { mode: p.choices.mode };
                if (p.choices.mode === 'inline' && p.choices.items) {
                  param.choices.values = p.choices.items.map((i: { id: string; name: string }) => `${i.id} (${i.name})`);
                } else if (p.choices.mode === 'remote') {
                  param.choices.hint = `Use get_param_choices tool with enrichment_id=${enrichment_id} and param_name="${p.name}" to browse available values`;
                }
              }
              return param;
            }),
            response_fields: enrichment.response_fields?.map(f => ({ name: f.name, type: f.type_field })),
            pagination: enrichment.pagination ?? { supported: false }
          };
          const paginationNote = enrichment.pagination?.supported
            ? `\n\nPagination: supported (${enrichment.pagination.per_page} results per page). Use the "pages" parameter in run_enrichment to fetch multiple pages.`
            : '';
          return {
            content: [{ type: 'text', text: safeResult(
              `Enrichment Details:\n\n${formatEnrichmentForDisplay(enrichment)}${paginationNote}\n\nFull Details:\n${JSON.stringify(details, null, 2)}`
            )}]
          };
        }

        case 'run_enrichment': {
          const { enrichment_id, params, skip_cache = false, pages = 1 } = args as {
            enrichment_id: number; params: Record<string, any>; skip_cache?: boolean; pages?: number;
          };
          const enrichment = await databarClient.getEnrichmentDetails(enrichment_id);
          const validation = validateParams(enrichment, params);
          if (!validation.valid) {
            auditLog({ timestamp: ts, tool: name, params: { enrichment_id }, result: 'error', message: 'validation failed' });
            return { content: [{ type: 'text', text: `Parameter validation failed:\n${validation.errors.join('\n')}` }], isError: true };
          }
          const paginationOpt = pages > 1 ? { pages } : undefined;
          if (!skip_cache && !paginationOpt) {
            const cachedData = cache.get(enrichment_id, params);
            if (cachedData) {
              auditLog({ timestamp: ts, tool: name, params: { enrichment_id }, estimatedCost: 0, result: 'cached' });
              return { content: [{ type: 'text', text: safeResult(
                `Enrichment completed (cached result)\n\nEnrichment: ${enrichment.name}\nCost: 0 credits (from cache)\n\nResults:\n${formatResults(cachedData)}`
              )}] };
            }
          }
          const estimatedCost = enrichment.price * (paginationOpt?.pages ?? 1);
          const guard = await guardSpending(name, estimatedCost, { enrichment_id });
          if (guard) return guard;
          const data = await databarClient.runEnrichmentSync(enrichment_id, params, paginationOpt);
          if (!paginationOpt) {
            cache.set(enrichment_id, params, data);
          }
          auditLog({ timestamp: ts, tool: name, params: { enrichment_id, pages }, estimatedCost, result: 'success' });
          const warn = unsafeModeWarning(spendingConfig, estimatedCost);
          const pagesNote = paginationOpt ? `\nPages requested: ${paginationOpt.pages}` : '';
          return { content: [{ type: 'text', text: safeResult(
            `${warn}Enrichment completed successfully\n\nEnrichment: ${enrichment.name}\nCost: ~${estimatedCost.toFixed(2)} credits${pagesNote}\n\nResults:\n${formatResults(data)}`
          )}] };
        }

        case 'run_bulk_enrichment': {
          const { enrichment_id, params_list, pages = 1 } = args as {
            enrichment_id: number; params_list: Record<string, any>[]; pages?: number;
          };
          const sizeErr = validateBulkArray(params_list, 'params_list');
          if (sizeErr) {
            auditLog({ timestamp: ts, tool: name, params: { enrichment_id, count: params_list?.length }, result: 'error', message: sizeErr });
            return { content: [{ type: 'text', text: sizeErr }], isError: true };
          }
          const enrichment = await databarClient.getEnrichmentDetails(enrichment_id);
          const paginationOpt = pages > 1 ? { pages } : undefined;
          const estimatedCost = enrichment.price * params_list.length * (paginationOpt?.pages ?? 1);
          const guard = await guardSpending(name, estimatedCost, { enrichment_id, count: params_list.length });
          if (guard) return guard;
          const data = await databarClient.runBulkEnrichmentSync(enrichment_id, params_list, paginationOpt);
          auditLog({ timestamp: ts, tool: name, params: { enrichment_id, count: params_list.length, pages }, estimatedCost, result: 'success' });
          const warn = unsafeModeWarning(spendingConfig, estimatedCost, params_list.length);
          const pagesNote = paginationOpt ? ` x ${paginationOpt.pages} pages` : '';
          return { content: [{ type: 'text', text: safeResult(
            `${warn}Bulk enrichment completed\n\nEnrichment: ${enrichment.name}\nRecords: ${params_list.length}${pagesNote}\nEstimated cost: ~${estimatedCost.toFixed(2)} credits\n\nResults:\n${formatResults(data)}`
          )}] };
        }

        case 'get_param_choices': {
          const { enrichment_id, param_name, q, page = 1, limit = 100 } = args as {
            enrichment_id: number; param_name: string; q?: string; page?: number; limit?: number;
          };
          auditLog({ timestamp: ts, tool: name, params: { enrichment_id, param_name, q, page, limit }, result: 'success' });
          const choices = await databarClient.getParamChoices(enrichment_id, param_name, { q, page, limit });
          const lines = choices.items.map(item => `- ${item.id}: ${item.name}`);
          const header = q
            ? `Choices for "${param_name}" (search: "${q}", page ${choices.page}/${Math.ceil(choices.total_count / choices.limit) || 1}, total: ${choices.total_count}):`
            : `Choices for "${param_name}" (page ${choices.page}/${Math.ceil(choices.total_count / choices.limit) || 1}, total: ${choices.total_count}):`;
          return {
            content: [{ type: 'text', text: safeResult(
              `${header}\n\n${lines.length > 0 ? lines.join('\n') : 'No choices found.'}${choices.has_next_page ? `\n\n(More results available — use page=${choices.page + 1})` : ''}`
            )}]
          };
        }

        case 'search_waterfalls': {
          const { query } = args as { query: string };
          auditLog({ timestamp: ts, tool: name, params: { query }, result: 'success' });
          const waterfalls = await databarClient.getAllWaterfalls();
          const lowerQuery = query.toLowerCase();
          const filtered = waterfalls.filter(w =>
            w.name.toLowerCase().includes(lowerQuery) ||
            w.description.toLowerCase().includes(lowerQuery) ||
            w.identifier.toLowerCase().includes(lowerQuery)
          );
          if (filtered.length === 0) {
            return { content: [{ type: 'text', text: `No waterfalls found matching "${query}".` }] };
          }
          return { content: [{ type: 'text', text: safeResult(
            `Found ${filtered.length} waterfall(s):\n\n${filtered.map(formatWaterfallForDisplay).join('\n\n---\n\n')}`
          )}] };
        }

        case 'run_waterfall': {
          const { waterfall_identifier, params, provider_ids, email_verifier } = args as {
            waterfall_identifier: string; params: Record<string, any>;
            provider_ids?: number[]; email_verifier?: number;
          };
          const waterfall = await databarClient.getWaterfallDetails(waterfall_identifier);
          const maxPrice = Math.max(...waterfall.available_enrichments.map(e => parseFloat(e.price)));
          const guard = await guardSpending(name, maxPrice, { waterfall_identifier });
          if (guard) return guard;
          const result = await databarClient.runWaterfallSync(waterfall_identifier, params, provider_ids, email_verifier);
          if (!result.data || result.data.length === 0) {
            auditLog({ timestamp: ts, tool: name, params: { waterfall_identifier }, result: 'success', message: 'no data' });
            return { content: [{ type: 'text', text: 'Waterfall completed but no data was found from any provider.' }] };
          }
          const resultData = result.data[0];
          const totalCost = resultData.steps.reduce((sum: number, step) => sum + parseFloat(step.cost), 0);
          auditLog({ timestamp: ts, tool: name, params: { waterfall_identifier }, estimatedCost: totalCost, result: 'success' });
          const warn = unsafeModeWarning(spendingConfig, totalCost);
          return { content: [{ type: 'text', text: safeResult(
            `${warn}Waterfall completed\n\nTotal Cost: ${totalCost.toFixed(2)} credits\n\nProviders Tried:\n${resultData.steps.map(s => `- ${s.provider}: ${s.result} (${s.cost} credits)`).join('\n')}\n\nResults:\n${formatResults(resultData.result)}`
          )}] };
        }

        case 'run_bulk_waterfall': {
          const { waterfall_identifier, params_list, provider_ids, email_verifier } = args as {
            waterfall_identifier: string; params_list: Record<string, any>[];
            provider_ids?: number[]; email_verifier?: number;
          };
          const sizeErr = validateBulkArray(params_list, 'params_list');
          if (sizeErr) {
            auditLog({ timestamp: ts, tool: name, params: { waterfall_identifier, count: params_list?.length }, result: 'error', message: sizeErr });
            return { content: [{ type: 'text', text: sizeErr }], isError: true };
          }
          const waterfall = await databarClient.getWaterfallDetails(waterfall_identifier);
          const maxPrice = Math.max(...waterfall.available_enrichments.map(e => parseFloat(e.price)));
          const estimatedCost = maxPrice * params_list.length;
          const guard = await guardSpending(name, estimatedCost, { waterfall_identifier, count: params_list.length });
          if (guard) return guard;
          const data = await databarClient.runBulkWaterfallSync(waterfall_identifier, params_list, provider_ids, email_verifier);
          auditLog({ timestamp: ts, tool: name, params: { waterfall_identifier, count: params_list.length }, estimatedCost, result: 'success' });
          const warn = unsafeModeWarning(spendingConfig, estimatedCost, params_list.length);
          return { content: [{ type: 'text', text: safeResult(
            `${warn}Bulk waterfall completed\n\nRecords: ${params_list.length}\nEstimated max cost: ~${estimatedCost.toFixed(2)} credits\n\nResults:\n${formatResults(data)}`
          )}] };
        }

        case 'create_table': {
          const { name: tableName, columns, rows = 0 } = args as {
            name?: string;
            columns?: string[];
            rows?: number;
          };
          const table = await databarClient.createTable({ name: tableName, columns, rows });
          auditLog({ timestamp: ts, tool: name, params: { name: tableName, columns, rows }, result: 'success' });
          return { content: [{ type: 'text', text: `Table created successfully\n\n${formatTableForDisplay(table)}` }] };
        }

        case 'list_tables': {
          const tables = await databarClient.getAllTables();
          auditLog({ timestamp: ts, tool: name, params: {}, result: 'success' });
          if (tables.length === 0) return { content: [{ type: 'text', text: 'No tables found in your workspace.' }] };
          return { content: [{ type: 'text', text: safeResult(
            `Found ${tables.length} table(s):\n\n${tables.map(formatTableForDisplay).join('\n\n---\n\n')}`
          )}] };
        }

        case 'get_table_columns': {
          const { table_uuid } = args as { table_uuid: string };
          const columns = await databarClient.getTableColumns(table_uuid);
          auditLog({ timestamp: ts, tool: name, params: { table_uuid }, result: 'success' });
          if (columns.length === 0) return { content: [{ type: 'text', text: 'No columns found on this table.' }] };
          return { content: [{ type: 'text', text: safeResult(
            `Table has ${columns.length} column(s):\n\n${columns.map(formatColumnForDisplay).join('\n')}`
          )}] };
        }

        case 'get_table_rows': {
          const { table_uuid, page = 1, per_page = 100, filter } = args as {
            table_uuid: string; page?: number; per_page?: number; filter?: Record<string, any>;
          };
          const clampedPerPage = Math.min(per_page, 500);
          const data = await databarClient.getTableRows(table_uuid, page, clampedPerPage, filter);
          auditLog({ timestamp: ts, tool: name, params: { table_uuid, page, per_page, filter }, result: 'success' });
          return { content: [{ type: 'text', text: safeResult(`Table rows (page ${page}):\n\n${formatResults(data)}`) }] };
        }

        case 'create_rows': {
          const { table_uuid, rows: rowsInput, records: recordsLegacy, options } = args as {
            table_uuid: string; rows?: { fields: Record<string, any> }[]; records?: { fields: Record<string, any> }[]; options?: any;
          };
          const inputRows = rowsInput || recordsLegacy;
          if (!inputRows) {
            return { content: [{ type: 'text', text: 'rows is required — provide an array of { fields: { column_name: value } } objects.' }], isError: true };
          }
          const sizeErr = validateRowBatchSize(inputRows, 'rows');
          if (sizeErr) return { content: [{ type: 'text', text: sizeErr }], isError: true };
          const response = await databarClient.createRows(table_uuid, { records: inputRows, options });
          auditLog({ timestamp: ts, tool: name, params: { table_uuid, count: inputRows.length }, result: 'success' });
          return { content: [{ type: 'text', text: `Create rows result:\n\n${formatCreateRowsResponse(response)}` }] };
        }

        case 'patch_rows': {
          const { table_uuid, rows, overwrite = true } = args as {
            table_uuid: string; rows: { id: string; fields: Record<string, any> }[]; overwrite?: boolean;
          };
          const sizeErr = validateRowBatchSize(rows, 'rows');
          if (sizeErr) return { content: [{ type: 'text', text: sizeErr }], isError: true };
          try {
            const response = await databarClient.patchRows(table_uuid, { rows, overwrite });
            auditLog({ timestamp: ts, tool: name, params: { table_uuid, count: rows.length }, result: 'success' });
            return { content: [{ type: 'text', text: `Patch rows result:\n\n${formatPatchRowsResponse(response)}` }] };
          } catch (patchErr: any) {
            if (patchErr.message?.includes('UNKNOWN_COLUMNS') || patchErr.message?.includes('unknown') || patchErr.message?.includes('column')) {
              try {
                const cols = await databarClient.getTableColumns(table_uuid);
                const colNames = cols.map(c => c.name).join(', ');
                throw new Error(`${patchErr.message}\n\nValid columns on this table: ${colNames}\n\nNote: patch_rows cannot create new columns. Use create_rows with options.allow_new_columns=true first.`);
              } catch (colErr: any) {
                if (colErr.message.includes('Valid columns')) throw colErr;
              }
            }
            throw patchErr;
          }
        }

        case 'upsert_rows': {
          const { table_uuid, rows } = args as {
            table_uuid: string; rows: { key: Record<string, any>; fields: Record<string, any> }[];
          };
          const sizeErr = validateRowBatchSize(rows, 'rows');
          if (sizeErr) return { content: [{ type: 'text', text: sizeErr }], isError: true };
          try {
            const response = await databarClient.upsertRows(table_uuid, { rows });
            auditLog({ timestamp: ts, tool: name, params: { table_uuid, count: rows.length }, result: 'success' });
            return { content: [{ type: 'text', text: `Upsert rows result:\n\n${formatUpsertRowsResponse(response)}` }] };
          } catch (upsertErr: any) {
            if (upsertErr.message?.includes('UNKNOWN_COLUMNS') || upsertErr.message?.includes('unknown') || upsertErr.message?.includes('column')) {
              try {
                const cols = await databarClient.getTableColumns(table_uuid);
                const colNames = cols.map(c => c.name).join(', ');
                throw new Error(`${upsertErr.message}\n\nValid columns on this table: ${colNames}\n\nNote: upsert_rows cannot create new columns. Use create_rows with options.allow_new_columns=true first.`);
              } catch (colErr: any) {
                if (colErr.message.includes('Valid columns')) throw colErr;
              }
            }
            throw upsertErr;
          }
        }

        case 'get_table_enrichments': {
          const { table_uuid } = args as { table_uuid: string };
          const enrichments = await databarClient.getTableEnrichments(table_uuid);
          auditLog({ timestamp: ts, tool: name, params: { table_uuid }, result: 'success' });
          if (enrichments.length === 0) return { content: [{ type: 'text', text: 'No enrichments configured on this table.' }] };
          return { content: [{ type: 'text', text: `Table has ${enrichments.length} enrichment(s):\n\n${enrichments.map(formatTableEnrichmentForDisplay).join('\n')}` }] };
        }

        case 'add_table_enrichment': {
          const { table_uuid, enrichment_id, mapping } = args as {
            table_uuid: string; enrichment_id: number; mapping: Record<string, any>;
          };

          // Auto-resolve column names → UUIDs for mapping-type entries
          const resolvedMapping: Record<string, any> = {};
          let columnMap: Record<string, string> | null = null;
          const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

          for (const [param, entry] of Object.entries(mapping)) {
            if (typeof entry !== 'object' || entry?.type !== 'mapping') {
              resolvedMapping[param] = entry;
              continue;
            }
            const value = String(entry.value || '');
            if (uuidPattern.test(value)) {
              resolvedMapping[param] = entry;
              continue;
            }
            if (!columnMap) {
              const cols = await databarClient.getTableColumns(table_uuid);
              columnMap = {};
              for (const c of cols) {
                columnMap[c.name] = c.identifier;
                columnMap[c.name.toLowerCase()] = c.identifier;
              }
            }
            const uuid = columnMap[value] || columnMap[value.toLowerCase()];
            if (uuid) {
              resolvedMapping[param] = { ...entry, value: uuid };
            } else {
              const validNames = Object.keys(columnMap).filter(k => !uuidPattern.test(k)).join(', ');
              return { content: [{ type: 'text', text: `Column "${value}" not found on this table.\n\nValid column names: ${validNames}\n\nUse one of these names in your mapping.` }], isError: true };
            }
          }

          try {
            // Snapshot before, so we can detect the new table-enrichment ID
            const beforeEnrichments = await databarClient.getTableEnrichments(table_uuid);
            const beforeIds = new Set(beforeEnrichments.map(e => e.id));

            await databarClient.addTableEnrichment(table_uuid, { enrichment: enrichment_id, mapping: resolvedMapping });

            // Fetch the updated list and surface the new table-enrichment ID
            const afterEnrichments = await databarClient.getTableEnrichments(table_uuid);
            const newEnrichments = afterEnrichments.filter(e => !beforeIds.has(e.id));
            const added = newEnrichments.length > 0 ? newEnrichments[0] : afterEnrichments[afterEnrichments.length - 1];

            auditLog({ timestamp: ts, tool: name, params: { table_uuid, enrichment_id }, result: 'success' });
            return { content: [{ type: 'text', text: safeResult(
              `Enrichment added to table successfully.\n\nTable enrichment ID: ${added?.id ?? 'unknown'}\nName: ${added?.name ?? 'unknown'}\n\nUse this table enrichment ID (${added?.id ?? '<id>'}) with run_table_enrichment to trigger a run.`
            )}] };
          } catch (addErr: any) {
            const msg = addErr.message || 'Unknown error';
            if (msg.includes('not found') || msg.includes('404')) {
              throw new Error(`Failed to add enrichment ${enrichment_id} to table. This enrichment may not support table mode — not all enrichments can be attached to tables. Try a different enrichment ID.`);
            }
            if (msg.includes('400') || msg.includes('Validation') || msg.includes('Invalid')) {
              const hint = columnMap
                ? `\n\nAvailable columns: ${Object.keys(columnMap).filter(k => !uuidPattern.test(k)).join(', ')}`
                : '';
              throw new Error(`Failed to add enrichment ${enrichment_id} to table: ${msg}${hint}\n\nCheck that the enrichment ID is correct and all required parameters are mapped.`);
            }
            throw addErr;
          }
        }

        case 'run_table_enrichment': {
          const { table_uuid, enrichment_id } = args as { table_uuid: string; enrichment_id: string };
          const guard = await guardSpending(name, 0, { table_uuid, enrichment_id });
          if (guard) return guard;
          const result = await databarClient.runTableEnrichment(table_uuid, enrichment_id);
          auditLog({ timestamp: ts, tool: name, params: { table_uuid, enrichment_id }, result: 'success' });
          return { content: [{ type: 'text', text:
            `Table enrichment/waterfall triggered successfully.\n\n` +
            `It is now running asynchronously on all applicable rows in the table. ` +
            `Results will appear as new columns on each row once processing completes.\n\n` +
            `To check progress, use get_table_rows to inspect the table — output columns will populate as rows are processed.`
          }] };
        }

        case 'add_table_waterfall': {
          const { table_uuid, waterfall_identifier, enrichments, mapping, email_verifier } = args as {
            table_uuid: string;
            waterfall_identifier: string;
            enrichments: number[];
            mapping: Record<string, string>;
            email_verifier?: number;
          };

          const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
          const resolvedMapping: Record<string, string> = {};
          let columnMap: Record<string, string> | null = null;

          for (const [param, colRef] of Object.entries(mapping)) {
            if (uuidPattern.test(colRef)) {
              resolvedMapping[param] = colRef;
              continue;
            }
            if (!columnMap) {
              const cols = await databarClient.getTableColumns(table_uuid);
              columnMap = {};
              for (const c of cols) {
                columnMap[c.name] = c.identifier;
                columnMap[c.name.toLowerCase()] = c.identifier;
                if (c.additional_intenal_name) {
                  columnMap[c.additional_intenal_name] = c.identifier;
                  columnMap[c.additional_intenal_name.toLowerCase()] = c.identifier;
                }
              }
            }
            const uuid = columnMap[colRef] || columnMap[colRef.toLowerCase()];
            if (uuid) {
              resolvedMapping[param] = uuid;
            } else {
              const validNames = Object.keys(columnMap).filter(k => !uuidPattern.test(k)).join(', ');
              return { content: [{ type: 'text', text: `Column "${colRef}" not found on this table.\n\nValid column names: ${validNames}\n\nUse one of these names in your mapping.` }], isError: true };
            }
          }

          try {
            const payload: any = {
              waterfall: waterfall_identifier,
              enrichments,
              mapping: resolvedMapping,
            };
            if (email_verifier != null) {
              payload.email_verifier = email_verifier;
            }

            const result = await databarClient.addTableWaterfall(table_uuid, payload);
            auditLog({ timestamp: ts, tool: name, params: { table_uuid, waterfall_identifier, enrichments }, result: 'success' });
            return { content: [{ type: 'text', text:
              `Waterfall added to table successfully.\n\n` +
              `Table waterfall ID: ${result.id}\n` +
              `Name: ${result.waterfall_name}\n\n` +
              `Use this ID (${result.id}) with run_table_enrichment to trigger a run on all rows.`
            }] };
          } catch (addErr: any) {
            const msg = addErr.message || 'Unknown error';
            if (msg.includes('not found') || msg.includes('404')) {
              throw new Error(`Failed to add waterfall "${waterfall_identifier}" to table. Check the waterfall identifier is correct (use search_waterfalls to find valid identifiers).`);
            }
            if (msg.includes('400') || msg.includes('Validation') || msg.includes('Invalid')) {
              const hint = columnMap
                ? `\n\nAvailable columns: ${Object.keys(columnMap).filter(k => !uuidPattern.test(k)).join(', ')}`
                : '';
              throw new Error(`Failed to add waterfall "${waterfall_identifier}" to table: ${msg}${hint}\n\nCheck that all required parameters are mapped to valid columns and enrichment IDs are from the waterfall's available_enrichments.`);
            }
            throw addErr;
          }
        }

        case 'get_table_waterfalls': {
          const { table_uuid } = args as { table_uuid: string };
          const waterfalls = await databarClient.getTableWaterfalls(table_uuid);
          auditLog({ timestamp: ts, tool: name, params: { table_uuid }, result: 'success' });
          if (waterfalls.length === 0) return { content: [{ type: 'text', text: 'No waterfalls installed on this table.' }] };
          const lines = waterfalls.map(w => `ID: ${w.id} — ${w.waterfall_name}`).join('\n');
          return { content: [{ type: 'text', text: `Table has ${waterfalls.length} waterfall(s):\n\n${lines}\n\nUse the ID with run_table_enrichment to trigger a run.` }] };
        }

        case 'get_user_balance': {
          const user = await databarClient.getUserInfo();
          auditLog({ timestamp: ts, tool: name, params: {}, result: 'success' });
          return {
            content: [{ type: 'text', text: `User Account Information:\n\nName: ${user.first_name || 'N/A'}\nEmail: ${user.email}\nBalance: ${user.balance} credits\nPlan: ${user.plan}` }]
          };
        }

        default:
          return { content: [{ type: 'text', text: `Unknown tool: ${name}` }], isError: true };
      }
    } catch (error: any) {
      auditLog({ timestamp: ts, tool: name, params: args as Record<string, any>, result: 'error', message: error.message });
      return {
        content: [{ type: 'text', text: `Error: ${error.message || 'Unknown error occurred'}` }],
        isError: true
      };
    }
  });

  return server;
}
