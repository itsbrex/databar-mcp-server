/**
 * Security guards: spending limits, input validation, result sanitization
 */

import { DatabarClient } from './databar-client.js';

export interface SpendingConfig {
  safeMode: boolean;
  maxCostPerRequest: number | null;
  minBalance: number;
  maxResultLength: number;
}

export function loadSpendingConfig(): SpendingConfig {
  const safeMode = process.env.DATABAR_SAFE_MODE;
  const maxCost = process.env.DATABAR_MAX_COST_PER_REQUEST;
  const minBal = process.env.DATABAR_MIN_BALANCE;
  const maxResult = process.env.DATABAR_MAX_RESULT_LENGTH;

  return {
    safeMode: safeMode !== 'false',
    maxCostPerRequest: maxCost ? parseFloat(maxCost) : null,
    minBalance: minBal ? parseFloat(minBal) : 1.0,
    maxResultLength: maxResult ? parseInt(maxResult) : 50000,
  };
}

/**
 * Check spending guard. Behavior depends on safe mode:
 *
 * Safe mode (default):
 *   - Checks cost cap (local, fast)
 *   - Fetches balance from API and blocks if insufficient
 *   - Returns error string if blocked, null if OK
 *
 * Unsafe mode (DATABAR_SAFE_MODE=false):
 *   - Still checks cost cap (local, fast)
 *   - Skips the balance API call entirely
 *   - Returns null (never blocks on balance)
 */
export async function checkSpendingGuard(
  client: DatabarClient,
  estimatedCost: number,
  config: SpendingConfig
): Promise<string | null> {
  if (config.maxCostPerRequest !== null && estimatedCost > config.maxCostPerRequest) {
    return `Blocked: estimated cost (${estimatedCost.toFixed(2)} credits) exceeds per-request limit (${config.maxCostPerRequest} credits). Adjust DATABAR_MAX_COST_PER_REQUEST to allow higher costs.`;
  }

  if (!config.safeMode) {
    return null;
  }

  try {
    const user = await client.getUserInfo();
    const balance = user.balance;

    if (balance < config.minBalance) {
      return `Blocked: current balance (${balance.toFixed(2)} credits) is below minimum threshold (${config.minBalance} credits). Top up your account or adjust DATABAR_MIN_BALANCE.`;
    }

    if (balance < estimatedCost) {
      return `Blocked: current balance (${balance.toFixed(2)} credits) is insufficient for estimated cost (${estimatedCost.toFixed(2)} credits).`;
    }
  } catch {
    // If balance check fails, don't block — the API will reject if truly insufficient
  }

  return null;
}

/**
 * Build a cost warning string for unsafe mode.
 * In safe mode, returns empty string (guard already checked).
 */
export function unsafeModeWarning(
  config: SpendingConfig,
  estimatedCost: number,
  recordCount?: number
): string {
  if (config.safeMode) return '';

  const parts = ['[Unsafe mode — no credit balance check]'];
  if (recordCount != null) {
    parts.push(`Records: ${recordCount}`);
  }
  parts.push(`Estimated cost: ~${estimatedCost.toFixed(2)} credits`);
  return parts.join(' | ') + '\n\n';
}

// ============================================================================
// Input Validation
// ============================================================================

const MAX_BULK_ITEMS = 100;
const API_BATCH_SIZE = 50;

export { API_BATCH_SIZE };

export function validateBulkSize(items: any[], label: string): string | null {
  if (!Array.isArray(items)) {
    return `${label} must be an array.`;
  }
  if (items.length === 0) {
    return `${label} cannot be empty.`;
  }
  if (items.length > MAX_BULK_ITEMS) {
    return `${label} has ${items.length} items, but the maximum is ${MAX_BULK_ITEMS} per request.`;
  }
  return null;
}

// ============================================================================
// Result Sanitization
// ============================================================================

export function sanitizeResult(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }
  const truncated = text.slice(0, maxLength);
  return truncated + `\n\n[Result truncated — showing ${maxLength.toLocaleString()} of ${text.length.toLocaleString()} characters. Use more specific parameters to reduce output size.]`;
}
