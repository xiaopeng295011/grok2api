import type { Env } from "./env";

/**
 * D1 (SQLite) 批量操作的变量限制
 * SQLite 默认限制为 999 个变量，D1 可能有更严格的限制
 * 设置为 50 作为安全值，避免 "too many SQL variables" 错误
 */
export const D1_BATCH_SIZE = 50;

/**
 * 分批执行 D1 batch 操作
 * 将大量 statement 分成小批次执行，避免超过 SQL 变量限制
 */
export async function batchExecute(
  db: Env["DB"],
  stmts: D1PreparedStatement[],
  batchSize: number = D1_BATCH_SIZE,
): Promise<void> {
  if (!stmts.length) return;

  for (let i = 0; i < stmts.length; i += batchSize) {
    const batch = stmts.slice(i, i + batchSize);
    await db.batch(batch);
  }
}

/**
 * 分批处理数组，用于 IN 子句等场景
 */
export async function batchProcess<T, R>(
  items: T[],
  batchSize: number,
  processor: (batch: T[]) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    results.push(await processor(batch));
  }
  return results;
}

export async function dbFirst<T>(
  db: Env["DB"],
  sql: string,
  params: unknown[] = [],
): Promise<T | null> {
  const stmt = db.prepare(sql).bind(...params);
  const row = await stmt.first<T>();
  return row ?? null;
}

export async function dbAll<T>(
  db: Env["DB"],
  sql: string,
  params: unknown[] = [],
): Promise<T[]> {
  const stmt = db.prepare(sql).bind(...params);
  const res = await stmt.all<T>();
  return res.results ?? [];
}

export async function dbRun(
  db: Env["DB"],
  sql: string,
  params: unknown[] = [],
): Promise<void> {
  const stmt = db.prepare(sql).bind(...params);
  await stmt.run();
}

