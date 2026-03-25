#!/usr/bin/env python3
"""
InsureYours Statistical Analysis — Tests whether insurance cost differences
are statistically significant, not just different averages.

Demonstrates: Hypothesis testing and confidence intervals (core DA skill).
Elevates the project from "Provider A averages less" to "Provider A is
significantly cheaper with 95% confidence."

Usage:
    pip install mysql-connector-python scipy
    python3 statistical_analysis.py --host localhost --user root --password pw
    python3 statistical_analysis.py --condition Diabetes   # filter by condition
    python3 statistical_analysis.py --output stats.txt     # save to file
"""

import argparse
import sys
from itertools import combinations

try:
    import mysql.connector
except ImportError:
    print("ERROR: mysql-connector-python required. pip install mysql-connector-python")
    sys.exit(1)

try:
    import math

    from scipy import stats
except ImportError:
    print("ERROR: scipy required. pip install scipy")
    sys.exit(1)

DATABASE = "Healthcare_Group_Project"


def section(title):
    return f"\n{'=' * 70}\n  {title}\n{'=' * 70}"


def run_analysis(host, port, user, password, condition_filter=None, output_file=None):
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=DATABASE,
    )
    cursor = conn.cursor(dictionary=True)

    lines = []

    def out(text=""):
        lines.append(text)
        print(text)

    out("INSUREYOURS STATISTICAL ANALYSIS")
    out("=" * 70)
    if condition_filter:
        out(f"  Filtered to condition: {condition_filter}")

    # ══════════════════════════════════════════════════════════════════
    # 1. PROVIDER COST COMPARISON — Welch's t-test
    # ══════════════════════════════════════════════════════════════════
    out(section("1. PAIRWISE PROVIDER COST COMPARISON (Welch's t-test)"))
    out("  Tests whether the billing difference between each pair of")
    out("  insurance providers is statistically significant (p < 0.05).")
    out("")

    # Get billing amounts per provider
    if condition_filter:
        cursor.execute(
            "SELECT InsuranceProvider, BillingAmount FROM HealthCare_Dataset"
            " WHERE MedicalCondition = %s ORDER BY InsuranceProvider",
            (condition_filter,),
        )
    else:
        cursor.execute(
            "SELECT InsuranceProvider, BillingAmount FROM HealthCare_Dataset"
            " ORDER BY InsuranceProvider"
        )
    rows = cursor.fetchall()

    provider_bills = {}
    for r in rows:
        provider_bills.setdefault(r["InsuranceProvider"], []).append(float(r["BillingAmount"]))

    providers = sorted(provider_bills.keys())

    if len(providers) < 2:
        out(f"  Only {len(providers)} provider(s) found — need at least 2 for comparison.")
        out("  Try running without --condition filter, or load more data.")
        cursor.close()
        conn.close()
        if output_file:
            with open(output_file, "w") as f:
                f.write("\n".join(lines))
        return

    out(f"  {'Provider':<25} {'N':>6} {'Mean':>12} {'StdDev':>12}")
    out(f"  {'-' * 25} {'-' * 6} {'-' * 12} {'-' * 12}")
    for p in providers:
        bills = provider_bills[p]
        mean = sum(bills) / len(bills)
        variance = sum((x - mean) ** 2 for x in bills) / (len(bills) - 1) if len(bills) > 1 else 0
        stddev = math.sqrt(variance)
        out(f"  {p:<25} {len(bills):>6} ${mean:>11,.2f} ${stddev:>11,.2f}")

    out("\n  Pairwise comparisons (Welch's t-test, unequal variances):")
    out(
        f"  {'Provider A':<20} {'Provider B':<20} {'Diff':>10} {'t-stat':>8} {'p-value':>10} {'Result'}"
    )
    out(f"  {'-' * 20} {'-' * 20} {'-' * 10} {'-' * 8} {'-' * 10} {'-' * 15}")

    sig_count = 0
    for p1, p2 in combinations(providers, 2):
        bills_1 = provider_bills[p1]
        bills_2 = provider_bills[p2]
        mean_diff = sum(bills_1) / len(bills_1) - sum(bills_2) / len(bills_2)

        t_stat, p_value = stats.ttest_ind(bills_1, bills_2, equal_var=False)
        significant = p_value < 0.05
        if significant:
            sig_count += 1
        result = "SIGNIFICANT" if significant else "not sig."
        out(f"  {p1:<20} {p2:<20} ${mean_diff:>9,.0f} {t_stat:>8.2f} {p_value:>10.4f} {result}")

    total_pairs = len(list(combinations(providers, 2)))
    out(f"\n  {sig_count}/{total_pairs} pairs have significantly different costs (p < 0.05)")

    # ══════════════════════════════════════════════════════════════════
    # 2. CONFIDENCE INTERVALS — 95% CI for each provider's mean billing
    # ══════════════════════════════════════════════════════════════════
    out(section("2. 95% CONFIDENCE INTERVALS FOR PROVIDER MEAN BILLING"))
    out("  If two providers' CIs don't overlap, their costs are likely")
    out("  significantly different.")
    out("")
    out(f"  {'Provider':<25} {'Mean':>12} {'95% CI Lower':>14} {'95% CI Upper':>14} {'Margin':>10}")
    out(f"  {'-' * 25} {'-' * 12} {'-' * 14} {'-' * 14} {'-' * 10}")

    for p in providers:
        bills = provider_bills[p]
        n = len(bills)
        mean = sum(bills) / n
        se = stats.sem(bills)
        ci = stats.t.interval(0.95, df=n - 1, loc=mean, scale=se)
        margin = ci[1] - mean
        out(f"  {p:<25} ${mean:>11,.2f} ${ci[0]:>13,.2f} ${ci[1]:>13,.2f} ${margin:>9,.2f}")

    # ══════════════════════════════════════════════════════════════════
    # 3. ANOVA — Are ANY provider costs significantly different?
    # ══════════════════════════════════════════════════════════════════
    out(section("3. ONE-WAY ANOVA — Overall Provider Difference Test"))
    out("  Tests whether at least one provider's mean cost differs")
    out("  from the others (omnibus test before pairwise comparisons).")
    out("")

    groups = [provider_bills[p] for p in providers]
    f_stat, p_value = stats.f_oneway(*groups)
    out(f"  F-statistic: {f_stat:.4f}")
    out(f"  p-value:     {p_value:.6f}")
    if p_value < 0.05:
        out("  Result:      SIGNIFICANT — at least one provider differs")
    else:
        out("  Result:      NOT SIGNIFICANT — no evidence of cost differences")

    # ══════════════════════════════════════════════════════════════════
    # 4. EFFECT SIZE — How big are the differences? (Cohen's d)
    # ══════════════════════════════════════════════════════════════════
    out(section("4. EFFECT SIZES (Cohen's d)"))
    out("  Measures practical significance, not just statistical significance.")
    out("  |d| < 0.2 = negligible, 0.2-0.5 = small, 0.5-0.8 = medium, > 0.8 = large")
    out("")
    cohens_label = "Cohen's d"
    out(f"  {'Provider A':<20} {'Provider B':<20} {cohens_label:>10} {'Effect'}")
    out(f"  {'-' * 20} {'-' * 20} {'-' * 10} {'-' * 12}")

    for p1, p2 in combinations(providers, 2):
        bills_1 = provider_bills[p1]
        bills_2 = provider_bills[p2]
        mean_1 = sum(bills_1) / len(bills_1)
        mean_2 = sum(bills_2) / len(bills_2)
        var_1 = sum((x - mean_1) ** 2 for x in bills_1) / (len(bills_1) - 1)
        var_2 = sum((x - mean_2) ** 2 for x in bills_2) / (len(bills_2) - 1)
        pooled_std = math.sqrt((var_1 + var_2) / 2)
        d = (mean_1 - mean_2) / pooled_std if pooled_std > 0 else 0
        abs_d = abs(d)
        if abs_d < 0.2:
            effect = "negligible"
        elif abs_d < 0.5:
            effect = "small"
        elif abs_d < 0.8:
            effect = "medium"
        else:
            effect = "LARGE"
        out(f"  {p1:<20} {p2:<20} {d:>10.4f} {effect}")

    # ══════════════════════════════════════════════════════════════════
    # 5. CONDITION-LEVEL BREAKDOWN
    # ══════════════════════════════════════════════════════════════════
    if not condition_filter:
        out(section("5. CONDITION-LEVEL ANOVA (Do costs vary by condition?)"))
        cursor.execute("""
            SELECT MedicalCondition, BillingAmount
            FROM HealthCare_Dataset
            ORDER BY MedicalCondition
        """)
        rows = cursor.fetchall()
        condition_bills = {}
        for r in rows:
            condition_bills.setdefault(r["MedicalCondition"], []).append(float(r["BillingAmount"]))

        conditions = sorted(condition_bills.keys())
        groups = [condition_bills[c] for c in conditions]
        f_stat, p_value = stats.f_oneway(*groups)

        out(f"\n  F-statistic: {f_stat:.4f}")
        out(f"  p-value:     {p_value:.6f}")
        if p_value < 0.05:
            out("  Result:      SIGNIFICANT — costs differ across conditions")
        else:
            out("  Result:      NOT SIGNIFICANT — no evidence costs differ by condition")

        out(f"\n  {'Condition':<25} {'N':>6} {'Mean':>12} {'95% CI':>28}")
        out(f"  {'-' * 25} {'-' * 6} {'-' * 12} {'-' * 28}")
        for c in conditions:
            bills = condition_bills[c]
            n = len(bills)
            mean = sum(bills) / n
            se = stats.sem(bills)
            ci = stats.t.interval(0.95, df=n - 1, loc=mean, scale=se)
            out(f"  {c:<25} {n:>6} ${mean:>11,.2f} [${ci[0]:>11,.2f} - ${ci[1]:>11,.2f}]")

    # ── Summary ──
    out(section("SUMMARY"))
    out("  This analysis goes beyond simple averages by testing whether")
    out("  observed cost differences are statistically reliable or could")
    out("  be due to random variation in the sample.")
    out("")
    out("  Key methods used:")
    out("    - Welch's t-test (pairwise, handles unequal variances)")
    out("    - 95% Confidence Intervals (precision of mean estimates)")
    out("    - One-way ANOVA (omnibus test across all providers)")
    out("    - Cohen's d (practical significance / effect size)")
    out("")
    out("=" * 70)

    cursor.close()
    conn.close()

    if output_file:
        with open(output_file, "w") as f:
            f.write("\n".join(lines))
        print(f"\nReport saved to: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="InsureYours Statistical Analysis")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--condition", default=None, help="Filter to one condition")
    parser.add_argument("--output", default=None, help="Save report to file")
    args = parser.parse_args()

    run_analysis(args.host, args.port, args.user, args.password, args.condition, args.output)
