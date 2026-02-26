/**
 * Security guards: spending limits, input validation, result sanitization
 */

import { DatabarClient } from './databar-client.js';

export interface SpendingConfig {
  maxCostPerRequest: number | null;
  minBalance: number;
  maxResultLength: number;
}

export function loadSpendingConfig(): SpendingConfig {
  const maxCost = process.env.DATABAR_MAX_COST_PER_REQUEST;
  const minBal = process.env.DATABAR_MIN_BALANCE;
  const maxResult = process.env.DATABAR_MAX_RESULT_LENGTH;

  return {
    maxCostPerRequest: maxCost ? parseFloat(maxCost) : null,
    minBalance: minBal ? parseFloat(minBal) : 1.0,
    maxResultLength: maxResult ? parseInt(maxResult) : 50000,
  };
}

/**
 * Check whether a tool call should be allowed based on estimated cost and balance.
 * Returns null if OK, or an error message string if blocked.
 */
export async function checkSpendingGuard(
  client: DatabarClient,
  estimatedCost: number,
  config: SpendingConfig
): Promise<string | null> {
  if (config.maxCostPerRequest !== null && estimatedCost > config.maxCostPerRequest) {
    return `Blocked: estimated cost (${estimatedCost.toFixed(2)} credits) exceeds per-request limit (${config.maxCostPerRequest} credits). Adjust DATABAR_MAX_COST_PER_REQUEST to allow higher costs.`;
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
    // If balance check fails, log but don't block — the API will reject if truly insufficient
  }

  return null;
}

// ============================================================================
// Input Validation
// ============================================================================

const MAX_BULK_ITEMS = 50;

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

/**
 * Truncate a result string to a safe length to prevent context overflow.
 */
export function sanitizeResult(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }
  const truncated = text.slice(0, maxLength);
  return truncated + `\n\n[Result truncated — showing ${maxLength.toLocaleString()} of ${text.length.toLocaleString()} characters. Use more specific parameters to reduce output size.]`;
}
