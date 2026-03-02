import sys

def main():
    target_file = sys.argv[1]
    with open(target_file, 'r', encoding='utf-8') as f:
        content = f.read()

    start_marker = '            # ── Cross-Brand Comparison Matrix ─────────────────────────────\n            st.markdown("#### > CROSS-BRAND EXECUTIVE MATRIX_")'
    end_marker = '            # ── Cross-Brand Cannibalization ──────────────────────────────'

    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)

    if start_idx == -1 or end_idx == -1:
        print("Markers not found", file=sys.stderr)
        print("start_idx:", start_idx, file=sys.stderr)
        print("end_idx:", end_idx, file=sys.stderr)
        sys.exit(1)

    new_code = """            # --- DYNAMIC BRAND DETECTION ---
            active_brands = sorted([b for b in financial_summary["brand"].unique() if b != "Combined"])
            combined_label = "All Business" if len(active_brands) > 2 else "Both Business"

            # ── Cross-Brand Executive Matrix ─────────────────────────────
            st.markdown("#### > CROSS-BRAND EXECUTIVE MATRIX_")
            st.markdown("*Insight: Tracks core revenue generation, operating margin safety, and top-line agency commissions across all entities.*")

            latest_month = both_business["month"].max()

            _mom_map = {
                "Turnover": "total_handle", "GGR": "ggr", "Margin %": "hold_pct",
                "Revenue (15%)": "revenue_share_deduction", "Conversions": "conversions",
                "Turnover / Player": "turnover_per_player", "Whale Risk %": None,
            }

            def _brand_snapshot(brand_name: str) -> dict:
                bdata = financial_summary[(financial_summary["brand"] == brand_name) & (financial_summary["month"] == latest_month)]
                if bdata.empty: return {}
                row = bdata.iloc[0]
                return {
                    "Turnover": float(row.get("total_handle", 0)), "GGR": float(row.get("ggr", 0)),
                    "Margin %": float(row.get("hold_pct", 0)), "Revenue (15%)": float(row.get("revenue_share_deduction", 0)),
                    "Conversions": int(row.get("conversions", 0)), "Turnover / Player": float(row.get("turnover_per_player", 0)),
                    "Whale Risk %": float(row.get("top_10_pct_ggr_share", 0)),
                }

            def _brand_mom(brand_name: str) -> list:
                bdata = financial_summary[financial_summary["brand"] == brand_name].sort_values("month")
                if bdata.empty: return ["-"] * len(metrics_list)
                brand_ts_m = _cached_time_series(bdata).get("monthly", pd.DataFrame())
                if brand_ts_m.empty: return ["-"] * len(metrics_list)
                latest = brand_ts_m.iloc[-1]
                return [f"{latest.get(f'{_mom_map.get(m)}_mom_pct'):+.1f}%" if pd.notna(latest.get(f"{_mom_map.get(m)}_mom_pct")) else "-" for m in metrics_list]

            def _bb_mom() -> list:
                if exec_ts_m.empty: return ["-"] * len(metrics_list)
                bb_ts_map = {"Turnover": "turnover", "GGR": "ggr", "Margin %": "margin", "Revenue (15%)": "revenue_share_deduction", "Conversions": "conversions", "Turnover / Player": "turnover_per_player"}
                return [f"{exec_latest.get(f'{bb_ts_map.get(m)}_mom_pct'):+.1f}%" if pd.notna(exec_latest.get(f"{bb_ts_map.get(m)}_mom_pct")) else "-" for m in metrics_list]

            def _brand_yoy(brand_name: str) -> list:
                bdata = financial_summary[financial_summary["brand"] == brand_name].sort_values("month")
                if bdata.empty: return ["-"] * len(metrics_list)
                brand_ts_m = _cached_time_series(bdata).get("monthly", pd.DataFrame())
                if brand_ts_m.empty: return ["-"] * len(metrics_list)
                latest = brand_ts_m.iloc[-1]
                return [f"{latest.get(f'{_mom_map.get(m)}_yoy_pct'):+.1f}%" if pd.notna(latest.get(f"{_mom_map.get(m)}_yoy_pct")) else "-" for m in metrics_list]

            def _bb_yoy() -> list:
                if exec_ts_m.empty: return ["-"] * len(metrics_list)
                bb_ts_map = {"Turnover": "turnover", "GGR": "ggr", "Margin %": "margin", "Revenue (15%)": "revenue_share_deduction", "Conversions": "conversions", "Turnover / Player": "turnover_per_player"}
                return [f"{exec_latest.get(f'{bb_ts_map.get(m)}_yoy_pct'):+.1f}%" if pd.notna(exec_latest.get(f"{bb_ts_map.get(m)}_yoy_pct")) else "-" for m in metrics_list]

            bb_snap = {
                "Turnover": float(exec_bb.get("turnover", 0)), "GGR": float(exec_bb.get("ggr", 0)),
                "Margin %": float(exec_bb.get("margin", 0)), "Revenue (15%)": float(exec_bb.get("revenue_share_deduction", 0)),
                "Conversions": int(exec_bb.get("conversions", 0)), "Turnover / Player": float(exec_bb.get("turnover_per_player", 0)),
                "Whale Risk %": e_whale,
            }

            metrics_list = ["Turnover", "GGR", "Margin %", "Revenue (15%)", "Conversions", "Turnover / Player", "Whale Risk %"]
            
            # Dynamically build the dictionary
            matrix_data = {
                "Metric": metrics_list,
                combined_label: [bb_snap.get(m, 0) for m in metrics_list],
                f"{combined_label} MoM": _bb_mom(),
                f"{combined_label} YoY": _bb_yoy(),
            }
            
            for brand in active_brands:
                snap = _brand_snapshot(brand)
                matrix_data[brand] = [snap.get(m, 0) for m in metrics_list]
                matrix_data[f"{brand} MoM"] = _brand_mom(brand)
                matrix_data[f"{brand} YoY"] = _brand_yoy(brand)

            # Dynamic column config mapping
            cfg = {"Metric": st.column_config.TextColumn("Metric"), combined_label: st.column_config.NumberColumn(combined_label, format="%.2f")}
            for brand in active_brands:
                cfg[brand] = st.column_config.NumberColumn(brand, format="%.2f")

            st.dataframe(pd.DataFrame(matrix_data), use_container_width=True, hide_index=True, column_config=cfg)

            # ── Brand vs Brand Trajectory ─────────────────────────────────
            st.markdown("#### > BRAND vs BRAND TRAJECTORY_")
            fig = go.Figure()
            colors = ["#FF4444", "#00FF41", "#1E90FF", "#FFD700", "#FF1493"]
            for i, brand in active_brands:
                b_ts = financial_summary[financial_summary["brand"] == brand][["month", "ggr"]].sort_values("month")
                fig.add_trace(go.Bar(x=b_ts["month"], y=b_ts["ggr"], name=brand, marker_color=colors[i % len(colors)]))
                
            fig.update_layout(barmode="group", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41",
                              legend=dict(font=dict(color="#00FF41")), xaxis=dict(gridcolor="#1a1a1a"), yaxis=dict(gridcolor="#1a1a1a", tickprefix="$", tickformat=",.0f"), margin=dict(l=0, r=0, t=30, b=0), height=400)
            st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": False})

            # ── Cross-Brand Demographics ─────────────────────────────────
            st.markdown("---")
            st.markdown("#### > CROSS-BRAND DEMOGRAPHICS_")
            demo_metrics = [("Total Active", "total_players"), ("Conversions", "conversions"), ("New Players", "new_players"), 
                            ("Reactivated", "reactivated_players"), ("Retained", "returning_players"), ("Profitable", "profitable_players"), ("Neg. Yield", "negative_yield_players")]

            demo_data = {"Metric": [label for label, _ in demo_metrics], combined_label: [int(exec_bb.get(col, 0)) for _, col in demo_metrics]}
            for brand in active_brands:
                bdata = financial_summary[(financial_summary["brand"] == brand) & (financial_summary["month"] == latest_month)]
                demo_data[brand] = [int(bdata.iloc[0].get(col, 0)) if not bdata.empty else 0 for _, col in demo_metrics]
            
            cfg_demo = {"Metric": st.column_config.TextColumn("Metric"), combined_label: st.column_config.NumberColumn(combined_label, format="%d")}
            for brand in active_brands: cfg_demo[brand] = st.column_config.NumberColumn(brand, format="%d")
            st.dataframe(pd.DataFrame(demo_data), use_container_width=True, hide_index=True, column_config=cfg_demo)

            # ── Cross-Brand VIP Health ────────────────────────────────────
            st.markdown("---")
            st.markdown("#### > CROSS-BRAND VIP HEALTH_")
            tier_labels, tier_search = ["True VIPs", "Churn Risk VIPs", "Casuals"], ["True VIP", "Churn Risk", "Casual"]

            def _vip_snap(raw_subset):
                rfm = _cached_rfm_summary(raw_subset, latest_month)
                if rfm.empty: return [0] * len(tier_labels)
                return [int(rfm.loc[rfm.iloc[:, 0].str.contains(s, na=False, case=False), rfm.columns[1]].sum()) if rfm.iloc[:, 0].str.contains(s, na=False, case=False).any() else 0 for s in tier_search]

            vip_data = {"Tier": tier_labels, combined_label: _vip_snap(df)}
            for brand in active_brands:
                vip_data[brand] = _vip_snap(df[df["brand"] == brand])

            cfg_vip = {"Tier": st.column_config.TextColumn("Tier"), combined_label: st.column_config.NumberColumn(combined_label, format="%d")}
            for brand in active_brands: cfg_vip[brand] = st.column_config.NumberColumn(brand, format="%d")
            st.dataframe(pd.DataFrame(vip_data), use_container_width=True, hide_index=True, column_config=cfg_vip)
\n"""

    new_content = content[:start_idx] + new_code + content[end_idx:]

    with open(target_file, 'w', encoding='utf-8') as f:
        f.write(new_content)

    print(f"Replaced block successfully from {start_idx} to {end_idx}")

if __name__ == "__main__":
    main()
