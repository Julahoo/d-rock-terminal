import datetime

def main():
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    with open("DEVLOG.md", "a", encoding="utf-8") as f:
        f.write(f"\n### [Hotfix - Data Hygiene] - {date_str}\n")
        f.write("- Merged redundant tags for Royal Panda (`ROYALPANDA` -> `RP`), Expekt (`EXPEKT` -> `EX`), and Rojabet (`ROJB` -> `ROJA`) directly in PostgreSQL `client_mapping` and historical tables.\n")
        f.write("- Corrected brand name typos: `Hahibi` -> `Bahibi` and `CASINODAYS` -> `CasinoDays`.\n")
        f.write("- Cleaned `BRAND_CODE_MAP` and `CLIENT_HIERARCHY` inside `src/ingestion.py` to prevent future drift.\n\n")

if __name__ == "__main__":
    main()
