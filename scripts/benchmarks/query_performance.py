#!/usr/bin/env python3
"""
Query Performance Benchmarking Script
======================================

Benchmarks query performance across different query engines (Trino, Spark, ClickHouse)
on the Iceberg tables.

Usage:
    python scripts/benchmarks/query_performance.py --engine all
    python scripts/benchmarks/query_performance.py --engine trino --iterations 5
    python scripts/benchmarks/query_performance.py --engine clickhouse --query simple
"""

import argparse
import json
import os
import statistics
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# Get credentials from environment variables
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "admin123")


@dataclass
class QueryResult:
    """Result of a query execution."""
    engine: str
    query_name: str
    query_type: str
    execution_time_ms: float
    row_count: int
    success: bool
    error: Optional[str] = None


@dataclass
class BenchmarkSuite:
    """Collection of benchmark results."""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    results: list = field(default_factory=list)

    def add_result(self, result: QueryResult):
        self.results.append(result)

    def summary(self) -> dict:
        """Generate summary statistics by engine and query type."""
        summary = {}
        for result in self.results:
            if not result.success:
                continue
            key = f"{result.engine}:{result.query_type}"
            if key not in summary:
                summary[key] = []
            summary[key].append(result.execution_time_ms)

        stats = {}
        for key, times in summary.items():
            stats[key] = {
                "count": len(times),
                "mean_ms": round(statistics.mean(times), 2),
                "median_ms": round(statistics.median(times), 2),
                "min_ms": round(min(times), 2),
                "max_ms": round(max(times), 2),
                "stddev_ms": round(statistics.stdev(times), 2) if len(times) > 1 else 0,
            }
        return stats


# =============================================================================
# Query Definitions
# =============================================================================

QUERIES = {
    "simple": {
        "name": "Simple Count",
        "description": "Count all rows in staging orders table",
        "trino": "SELECT COUNT(*) as cnt FROM iceberg.staging.stg_shopify_orders",
        "spark": "SELECT COUNT(*) as cnt FROM iceberg.staging.stg_shopify_orders",
        "clickhouse": "SELECT COUNT(*) as cnt FROM iceberg.stg_shopify_orders",
    },
    "filter": {
        "name": "Filtered Aggregation",
        "description": "Count orders by status",
        "trino": """
            SELECT order_status, COUNT(*) as cnt, SUM(total_price) as total
            FROM iceberg.staging.stg_shopify_orders
            GROUP BY order_status
            ORDER BY total DESC
        """,
        "spark": """
            SELECT order_status, COUNT(*) as cnt, SUM(total_price) as total
            FROM iceberg.staging.stg_shopify_orders
            GROUP BY order_status
            ORDER BY total DESC
        """,
        "clickhouse": """
            SELECT order_status, COUNT(*) as cnt, SUM(total_price) as total
            FROM iceberg.stg_shopify_orders
            GROUP BY order_status
            ORDER BY total DESC
        """,
    },
    "join": {
        "name": "Multi-Table Join",
        "description": "Join orders with customers via entity index",
        "trino": """
            SELECT
                c.customer_email,
                COUNT(DISTINCT o.order_id) as order_count,
                SUM(o.total_price) as total_spent
            FROM iceberg.staging.stg_shopify_orders o
            JOIN iceberg.semantic.entity_index e ON o.customer_id = CAST(e.source_id AS BIGINT)
            JOIN iceberg.staging.stg_shopify_customers c ON c.customer_id = CAST(e.source_id AS BIGINT)
            WHERE e.source = 'shopify' AND e.entity_type = 'customer'
            GROUP BY c.customer_email
            ORDER BY total_spent DESC
            LIMIT 10
        """,
        "spark": """
            SELECT
                c.customer_email,
                COUNT(DISTINCT o.order_id) as order_count,
                SUM(o.total_price) as total_spent
            FROM iceberg.staging.stg_shopify_orders o
            JOIN iceberg.semantic.entity_index e ON o.customer_id = CAST(e.source_id AS BIGINT)
            JOIN iceberg.staging.stg_shopify_customers c ON c.customer_id = CAST(e.source_id AS BIGINT)
            WHERE e.source = 'shopify' AND e.entity_type = 'customer'
            GROUP BY c.customer_email
            ORDER BY total_spent DESC
            LIMIT 10
        """,
        "clickhouse": """
            SELECT
                c.customer_email,
                COUNT(DISTINCT o.order_id) as order_count,
                SUM(o.total_price) as total_spent
            FROM iceberg.stg_shopify_orders o
            JOIN iceberg.entity_index e ON o.customer_id = toInt64(e.source_id)
            JOIN iceberg.stg_shopify_customers c ON c.customer_id = toInt64(e.source_id)
            WHERE e.source = 'shopify' AND e.entity_type = 'customer'
            GROUP BY c.customer_email
            ORDER BY total_spent DESC
            LIMIT 10
        """,
    },
    "analytics": {
        "name": "Analytics Query",
        "description": "Complex aggregation with window functions",
        "trino": """
            SELECT
                DATE(created_at) as order_date,
                COUNT(*) as orders,
                SUM(total_price) as daily_revenue,
                AVG(total_price) as avg_order_value,
                SUM(SUM(total_price)) OVER (ORDER BY DATE(created_at)) as cumulative_revenue
            FROM iceberg.staging.stg_shopify_orders
            GROUP BY DATE(created_at)
            ORDER BY order_date
        """,
        "spark": """
            SELECT
                DATE(created_at) as order_date,
                COUNT(*) as orders,
                SUM(total_price) as daily_revenue,
                AVG(total_price) as avg_order_value,
                SUM(SUM(total_price)) OVER (ORDER BY DATE(created_at)) as cumulative_revenue
            FROM iceberg.staging.stg_shopify_orders
            GROUP BY DATE(created_at)
            ORDER BY order_date
        """,
        "clickhouse": """
            SELECT
                toDate(created_at) as order_date,
                COUNT(*) as orders,
                SUM(total_price) as daily_revenue,
                AVG(total_price) as avg_order_value,
                SUM(daily_revenue) OVER (ORDER BY order_date) as cumulative_revenue
            FROM iceberg.stg_shopify_orders
            GROUP BY order_date
            ORDER BY order_date
        """,
    },
    "time_travel": {
        "name": "Time Travel Query",
        "description": "Query historical snapshot (Trino/Spark only)",
        "trino": """
            SELECT COUNT(*) as cnt
            FROM iceberg.staging.stg_shopify_orders
            FOR VERSION AS OF 1
        """,
        "spark": """
            SELECT COUNT(*) as cnt
            FROM iceberg.staging.stg_shopify_orders VERSION AS OF 1
        """,
        "clickhouse": None,  # ClickHouse doesn't support Iceberg time travel
    },
}


# =============================================================================
# Query Executors
# =============================================================================

def run_trino_query(query: str) -> tuple[float, int, Optional[str]]:
    """Execute query via Trino CLI and return (time_ms, row_count, error)."""
    start = time.perf_counter()
    try:
        result = subprocess.run(
            [
                "docker", "exec", "iceberg-trino",
                "trino", "--execute", query
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000

        if result.returncode != 0:
            return elapsed_ms, 0, result.stderr.strip()

        # Count output lines (excluding header for formatted output)
        lines = [l for l in result.stdout.strip().split("\n") if l]
        row_count = len(lines) if lines else 0
        return elapsed_ms, row_count, None

    except subprocess.TimeoutExpired:
        return 60000, 0, "Query timeout"
    except Exception as e:
        return 0, 0, str(e)


def run_spark_query(query: str) -> tuple[float, int, Optional[str]]:
    """Execute query via Spark SQL and return (time_ms, row_count, error)."""
    # Escape the query for shell
    escaped_query = query.replace('"', '\\"').replace("'", "'\\''")

    start = time.perf_counter()
    try:
        result = subprocess.run(
            [
                "docker", "exec", "iceberg-spark-master",
                "/opt/spark/bin/spark-sql",
                "--conf", "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
                "--conf", "spark.sql.catalog.iceberg.type=rest",
                "--conf", "spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181",
                "--conf", "spark.sql.catalog.iceberg.warehouse=s3a://warehouse/",
                "--conf", "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
                "--conf", "spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000",
                "--conf", f"spark.sql.catalog.iceberg.s3.access-key-id={MINIO_ROOT_USER}",
                "--conf", f"spark.sql.catalog.iceberg.s3.secret-access-key={MINIO_ROOT_PASSWORD}",
                "--conf", "spark.sql.catalog.iceberg.s3.path-style-access=true",
                "-e", query,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000

        if result.returncode != 0:
            return elapsed_ms, 0, result.stderr.strip()[:200]

        # Count result rows (Spark SQL output includes header)
        lines = [l for l in result.stdout.strip().split("\n") if l and not l.startswith("Time taken")]
        row_count = max(0, len(lines) - 1)  # Subtract header
        return elapsed_ms, row_count, None

    except subprocess.TimeoutExpired:
        return 120000, 0, "Query timeout"
    except Exception as e:
        return 0, 0, str(e)


def run_clickhouse_query(query: str) -> tuple[float, int, Optional[str]]:
    """Execute query via ClickHouse client and return (time_ms, row_count, error)."""
    start = time.perf_counter()
    try:
        result = subprocess.run(
            [
                "docker", "exec", "iceberg-clickhouse",
                "clickhouse-client",
                "--query", query,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000

        if result.returncode != 0:
            return elapsed_ms, 0, result.stderr.strip()

        lines = [l for l in result.stdout.strip().split("\n") if l]
        row_count = len(lines) if lines else 0
        return elapsed_ms, row_count, None

    except subprocess.TimeoutExpired:
        return 60000, 0, "Query timeout"
    except Exception as e:
        return 0, 0, str(e)


EXECUTORS = {
    "trino": run_trino_query,
    "spark": run_spark_query,
    "clickhouse": run_clickhouse_query,
}


# =============================================================================
# Main Benchmark Runner
# =============================================================================

def run_benchmark(
    engines: list[str],
    query_types: list[str],
    iterations: int = 3,
    warmup: bool = True,
) -> BenchmarkSuite:
    """Run benchmark suite across engines and queries."""
    suite = BenchmarkSuite()

    for query_type in query_types:
        query_def = QUERIES.get(query_type)
        if not query_def:
            print(f"Unknown query type: {query_type}")
            continue

        print(f"\n{'='*60}")
        print(f"Query: {query_def['name']}")
        print(f"Description: {query_def['description']}")
        print(f"{'='*60}")

        for engine in engines:
            query = query_def.get(engine)
            if query is None:
                print(f"  [{engine}] Not supported for this query type")
                continue

            executor = EXECUTORS.get(engine)
            if not executor:
                print(f"  [{engine}] Unknown engine")
                continue

            print(f"\n  [{engine}] Running {iterations} iterations...")

            # Warmup run
            if warmup:
                print(f"    Warmup...", end=" ", flush=True)
                executor(query)
                print("done")

            # Benchmark runs
            for i in range(iterations):
                time_ms, row_count, error = executor(query)
                result = QueryResult(
                    engine=engine,
                    query_name=query_def["name"],
                    query_type=query_type,
                    execution_time_ms=time_ms,
                    row_count=row_count,
                    success=error is None,
                    error=error,
                )
                suite.add_result(result)

                status = "✓" if result.success else "✗"
                print(f"    Run {i+1}: {status} {time_ms:.1f}ms ({row_count} rows)")
                if error:
                    print(f"       Error: {error[:100]}")

    return suite


def print_summary(suite: BenchmarkSuite):
    """Print benchmark summary table."""
    print(f"\n{'='*80}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*80}")
    print(f"Timestamp: {suite.timestamp}")
    print(f"Total runs: {len(suite.results)}")
    print()

    stats = suite.summary()
    if not stats:
        print("No successful results to summarize.")
        return

    # Print table header
    print(f"{'Engine:Query Type':<30} {'Count':>6} {'Mean':>10} {'Median':>10} {'Min':>10} {'Max':>10}")
    print("-" * 80)

    for key, data in sorted(stats.items()):
        print(f"{key:<30} {data['count']:>6} {data['mean_ms']:>10.1f} {data['median_ms']:>10.1f} {data['min_ms']:>10.1f} {data['max_ms']:>10.1f}")

    print()

    # Performance comparison
    print("Performance Comparison (lower is better):")
    print("-" * 40)

    query_types = set(k.split(":")[1] for k in stats.keys())
    for qt in sorted(query_types):
        engines_for_query = {k.split(":")[0]: v["median_ms"] for k, v in stats.items() if k.endswith(f":{qt}")}
        if len(engines_for_query) > 1:
            fastest = min(engines_for_query.items(), key=lambda x: x[1])
            print(f"  {qt}: {fastest[0]} is fastest ({fastest[1]:.1f}ms)")


def main():
    parser = argparse.ArgumentParser(description="Benchmark query performance across engines")
    parser.add_argument(
        "--engine",
        choices=["trino", "spark", "clickhouse", "all"],
        default="all",
        help="Engine to benchmark (default: all)",
    )
    parser.add_argument(
        "--query",
        choices=list(QUERIES.keys()) + ["all"],
        default="all",
        help="Query type to benchmark (default: all)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Number of iterations per query (default: 3)",
    )
    parser.add_argument(
        "--no-warmup",
        action="store_true",
        help="Skip warmup run",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file for JSON results",
    )
    args = parser.parse_args()

    # Determine engines and queries
    engines = ["trino", "spark", "clickhouse"] if args.engine == "all" else [args.engine]
    queries = list(QUERIES.keys()) if args.query == "all" else [args.query]

    print("=" * 80)
    print("ICEBERG QUERY PERFORMANCE BENCHMARK")
    print("=" * 80)
    print(f"Engines: {', '.join(engines)}")
    print(f"Queries: {', '.join(queries)}")
    print(f"Iterations: {args.iterations}")
    print(f"Warmup: {not args.no_warmup}")

    # Run benchmarks
    suite = run_benchmark(
        engines=engines,
        query_types=queries,
        iterations=args.iterations,
        warmup=not args.no_warmup,
    )

    # Print summary
    print_summary(suite)

    # Save results
    if args.output:
        output_data = {
            "timestamp": suite.timestamp,
            "config": {
                "engines": engines,
                "queries": queries,
                "iterations": args.iterations,
            },
            "results": [
                {
                    "engine": r.engine,
                    "query_name": r.query_name,
                    "query_type": r.query_type,
                    "execution_time_ms": r.execution_time_ms,
                    "row_count": r.row_count,
                    "success": r.success,
                    "error": r.error,
                }
                for r in suite.results
            ],
            "summary": suite.summary(),
        }
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output}")


if __name__ == "__main__":
    main()
