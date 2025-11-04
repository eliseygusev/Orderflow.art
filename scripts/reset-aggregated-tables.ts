#!/usr/bin/env bun
import { createClient } from "@clickhouse/client-web";

const clickhouse = createClient({
  host: process.env.CLICKHOUSE_HOST!,
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD!,
  database: "orderflow",
});

async function main() {
  console.log("🗑️  Resetting aggregated tables...\n");

  try {
    // Test connection
    console.log("Testing ClickHouse connection...");
    await clickhouse.ping();
    console.log("✓ Connected to ClickHouse\n");

    // Drop existing aggregated tables
    console.log("Dropping old aggregated tables...");
    await clickhouse.exec({
      query: "DROP TABLE IF EXISTS orderflow.prodof_aggregated",
    });
    console.log("✓ Dropped orderflow.prodof_aggregated");

    await clickhouse.exec({
      query: "DROP TABLE IF EXISTS orderflow.prodlq_aggregated",
    });
    console.log("✓ Dropped orderflow.prodlq_aggregated\n");

    // Recreate tables with new schema
    console.log("Creating new aggregated tables...");

    await clickhouse.exec({
      query: `
        CREATE TABLE IF NOT EXISTS orderflow.prodof_aggregated (
          frontend String,
          metaaggregator String,
          solver String,
          mempool String,
          ofa String,
          builder String,
          total_volume Float64
        ) ENGINE = SummingMergeTree(total_volume)
        ORDER BY (frontend, metaaggregator, solver, mempool, ofa, builder)
      `,
    });
    console.log("✓ Created orderflow.prodof_aggregated");

    await clickhouse.exec({
      query: `
        CREATE TABLE IF NOT EXISTS orderflow.prodlq_aggregated (
          frontend String,
          metaaggregator String,
          solver String,
          aggregator String,
          liquidity_src String,
          pmm String,
          total_volume Float64
        ) ENGINE = SummingMergeTree(total_volume)
        ORDER BY (frontend, metaaggregator, solver, aggregator, liquidity_src, pmm)
      `,
    });
    console.log("✓ Created orderflow.prodlq_aggregated\n");

    // Verify tables exist
    console.log("Verifying tables...");
    const ofResult = await clickhouse.query({
      query: "SELECT COUNT(*) as count FROM orderflow.prodof_aggregated",
      format: "JSONEachRow",
    });
    const ofData = (await ofResult.json()) as Array<{ count: string }>;
    console.log(`✓ prodof_aggregated: ${ofData[0].count} rows`);

    const lqResult = await clickhouse.query({
      query: "SELECT COUNT(*) as count FROM orderflow.prodlq_aggregated",
      format: "JSONEachRow",
    });
    const lqData = (await lqResult.json()) as Array<{ count: string }>;
    console.log(`✓ prodlq_aggregated: ${lqData[0].count} rows`);

    console.log("\n✅ Tables reset successfully!");
    console.log("\nNext step: Run 'bun run sync:aggregated' to populate with fresh data");

    process.exit(0);
  } catch (error) {
    console.error("\n✗ Error:", error);
    process.exit(1);
  } finally {
    await clickhouse.close();
  }
}

// Run main function
main();
