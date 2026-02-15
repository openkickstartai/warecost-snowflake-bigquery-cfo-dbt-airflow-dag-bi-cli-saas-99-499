"""WareCost — Data warehouse query cost attribution & budget enforcement engine."""
import json
import statistics
import sys
from dataclasses import dataclass
from typing import Optional

import click
from rich.console import Console
from rich.table import Table


@dataclass
class QueryRecord:
    query_id: str
    query_text: str
    user_name: str
    warehouse_name: str
    credits_used: float
    bytes_scanned: int
    execution_time_ms: int
    start_time: str
    query_tag: str = ""

    def _extract(self, prefix: str) -> Optional[str]:
        if prefix in self.query_tag:
            return self.query_tag.split(prefix)[1].split(";")[0].strip()
        return None

    @property
    def team(self) -> str:
        return self._extract("team=") or (
            self.user_name.split("_")[0] if "_" in self.user_name else "unattributed"
        )

    @property
    def dbt_model(self) -> Optional[str]:
        return self._extract("dbt:") or self._extract("model=")

    @property
    def dag_id(self) -> Optional[str]:
        return self._extract("dag=")

    @property
    def cost_usd(self) -> float:
        return round(self.credits_used * 3.0, 4)


class CostEngine:
    def __init__(self, credit_price: float = 3.0):
        self.credit_price = credit_price
        self.queries: list[QueryRecord] = []
        self.budgets: dict[str, float] = {}

    def load(self, records: list[dict]) -> int:
        self.queries = [QueryRecord(**r) for r in records]
        return len(self.queries)

    def set_budget(self, team: str, amount: float):
        self.budgets[team] = amount

    def breakdown(self, dim: str) -> dict:
        groups: dict[str, list[QueryRecord]] = {}
        for q in self.queries:
            key = getattr(q, dim, None) or "unattributed"
            groups.setdefault(key, []).append(q)
        result = {}
        for k, qs in sorted(groups.items(), key=lambda x: -sum(q.credits_used for q in x[1])):
            tc = sum(q.credits_used for q in qs)
            result[k] = {
                "queries": len(qs), "credits": round(tc, 4),
                "cost_usd": round(tc * self.credit_price, 2),
                "bytes": sum(q.bytes_scanned for q in qs),
            }
        return result

    def anomalies(self, z_thresh: float = 2.0) -> list[dict]:
        if len(self.queries) < 3:
            return []
        costs = [q.cost_usd for q in self.queries]
        mu, sd = statistics.mean(costs), statistics.stdev(costs)
        if sd == 0:
            return []
        return sorted(
            [{"query_id": q.query_id, "cost_usd": q.cost_usd,
              "z_score": round((q.cost_usd - mu) / sd, 2),
              "team": q.team, "warehouse": q.warehouse_name}
             for q in self.queries if (q.cost_usd - mu) / sd > z_thresh],
            key=lambda x: -x["z_score"],
        )

    def budget_alerts(self) -> list[dict]:
        by_team = self.breakdown("team")
        alerts = []
        for team, limit in self.budgets.items():
            spent = by_team.get(team, {}).get("cost_usd", 0)
            pct = round(spent / limit * 100, 1) if limit > 0 else 0
            if pct >= 80:
                alerts.append({"team": team, "budget": limit, "spent": spent,
                               "pct": pct, "status": "OVER" if pct >= 100 else "WARNING"})
        return alerts

    def summary(self) -> dict:
        tc = sum(q.credits_used for q in self.queries)
        return {"total_queries": len(self.queries), "total_credits": round(tc, 4),
                "total_cost_usd": round(tc * self.credit_price, 2),
                "by_team": self.breakdown("team"),
                "by_warehouse": self.breakdown("warehouse_name"),
                "anomalies": self.anomalies(), "budget_alerts": self.budget_alerts()}


console = Console()


@click.group()
def cli():
    """WareCost — Data warehouse query cost attribution."""


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--budget", "-b", multiple=True, help="team:amount e.g. analytics:500")
def analyze(file: str, budget: tuple[str, ...]):
    """Analyze query history from a JSON file."""
    engine = CostEngine()
    with open(file) as f:
        n = engine.load(json.load(f))
    for b in budget:
        t, a = b.split(":")
        engine.set_budget(t.strip(), float(a))
    report = engine.summary()
    console.print(f"\n[bold]WareCost Report[/bold] — {n} queries analyzed")
    console.print(f"💰 Total: [bold red]${report['total_cost_usd']}[/bold red] "
                  f"({report['total_credits']} credits)\n")
    tbl = Table(title="Cost by Team")
    for col in ["Team", "Queries", "Credits", "Cost ($)", "Bytes"]:
        tbl.add_column(col, justify="right" if col != "Team" else "left")
    for team, d in report["by_team"].items():
        tbl.add_row(team, str(d["queries"]), str(d["credits"]),
                    f"${d['cost_usd']}", f"{d['bytes']:,}")
    console.print(tbl)
    for a in report["anomalies"][:5]:
        console.print(f"[red]⚠ ANOMALY[/red] {a['query_id']}: ${a['cost_usd']} "
                      f"(z={a['z_score']}) [{a['team']}]")
    for a in report["budget_alerts"]:
        icon = "🔴" if a["status"] == "OVER" else "🟡"
        console.print(f"{icon} {a['team']}: ${a['spent']}/${a['budget']} "
                      f"({a['pct']}%) [{a['status']}]")


if __name__ == "__main__":
    cli()
