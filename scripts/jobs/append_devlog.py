import datetime

def main():
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    with open("DEVLOG.md", "a", encoding="utf-8") as f:
        f.write(f"### [Feature - True CAC Refinements] - {date_str}\n")
        f.write("- Expanded `ops_historical_benchmarks` schema to store average daily telecom costs and True CAC baselines.\n")
        f.write("- Upgraded the benchmark generator script to calculate historical CAC signatures.\n")
        f.write("- Refined the True Cost-Per-Outcome Leaderboard in `app.py` to display dynamic `CAC Delta` columns, instantly highlighting campaigns bleeding telecom margins vs. 6-month averages.\n")
        f.write("- Integrated newly registered brands (Wetigo, Hahibi, NitroCasino) into the DB mapping layer and updated category names to full names (Casino, Sportsbook, etc.).\n\n")

if __name__ == "__main__":
    main()
