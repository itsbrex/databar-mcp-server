/**
 * Audit logging for tool calls.
 * Logs to stderr (standard for MCP servers) and optionally to a file.
 */

import { appendFileSync } from 'fs';

const LOG_FILE = process.env.DATABAR_AUDIT_LOG || null;

interface AuditEntry {
  timestamp: string;
  tool: string;
  params: Record<string, any>;
  estimatedCost?: number;
  result: 'success' | 'error' | 'blocked' | 'cached';
  message?: string;
}

function redactSensitive(params: Record<string, any>): Record<string, any> {
  const redacted = { ...params };
  for (const key of Object.keys(redacted)) {
    const lower = key.toLowerCase();
    if (lower.includes('apikey') || lower.includes('api_key') || lower.includes('password') || lower.includes('secret') || lower.includes('token')) {
      redacted[key] = '[REDACTED]';
    }
  }
  return redacted;
}

export function auditLog(entry: AuditEntry): void {
  const safeEntry: AuditEntry = {
    ...entry,
    params: redactSensitive(entry.params),
  };

  const line = JSON.stringify(safeEntry);
  console.error(`[audit] ${line}`);

  if (LOG_FILE) {
    try {
      appendFileSync(LOG_FILE, line + '\n');
    } catch {
      // Don't crash the server if log file write fails
    }
  }
}
