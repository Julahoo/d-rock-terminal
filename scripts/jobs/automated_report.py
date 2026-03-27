import os
import sys
import smtplib
from email.message import EmailMessage
from email.utils import make_msgid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import base64
import io
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv("DATABASE_URL", "")
if "@db:" in db_url:
    os.environ["DATABASE_URL"] = db_url.replace("@db:", "@localhost:")

# Add project root to python path to access src.database
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.database import engine

def fetch_data(start_date, end_date):
    # Apply strict NLI engagement filter universally across all metrics to prevent log/conv inflation
    query = f"""
        SELECT 
            ops_date, ops_client as client, extracted_lifecycle as lifecycle,
            SUM("Records") as records, 
            SUM("KPI2-Login") as logins, 
            SUM("KPI1-Conv.") as conversions,
            SUM("Total_Campaign_Cost") as total_cost
        FROM ops_telemarketing_data_materialized
        WHERE ops_date >= '{start_date}' AND ops_date <= '{end_date}'
        AND extracted_engagement = 'NLI'
        GROUP BY ops_date, ops_client, extracted_lifecycle
    """
    return pd.read_sql(query, engine)

def fetch_slas():
    # Only pull positive monthly limits, aggregate to client level
    return pd.read_sql("SELECT client_name as client, SUM(monthly_minimum_records) as sla_limit FROM contractual_volumes WHERE monthly_minimum_records > 0 GROUP BY client_name", engine)

def create_chart_base64(df, date_col, metric_col, title, color):
    if df.empty: return ""
    
    # Ensure all 30 dates exist
    df = df.set_index(date_col).asfreq('D', fill_value=0).reset_index()
    
    avg_30 = df[metric_col].mean()
    avg_7 = df.tail(7)[metric_col].mean()
    yesterday_val = df.iloc[-1][metric_col] if not df.empty else 0
    
    fig = go.Figure()
    # Bar chart for Volume, Line for the rest
    if 'Volume' in title:
        fig.add_trace(go.Bar(x=df[date_col], y=df[metric_col], name=metric_col, marker_color=color))
    else:
        fig.add_trace(go.Scatter(x=df[date_col], y=df[metric_col], mode='lines+markers', name=metric_col, line=dict(color=color, width=3)))
        
    fig.add_hline(y=avg_30, line_dash="solid", line_color="red", annotation_text="30-Day Avg", annotation_position="top left")
    fig.add_hline(y=avg_7, line_dash="dash", line_color="orange", annotation_text="7-Day Avg", annotation_position="top right")
    
    fig.update_layout(
        title=title, template='plotly_white', height=300, margin=dict(l=40, r=40, t=40, b=40),
        xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor='#eee')
    )
    
    img_bytes = fig.to_image(format="png", engine="kaleido")
    
    # Use Content-ID instead of massive inline Base64 strings to bypass Gmail strict sizing logic
    # Explicitly enforce the domain to prevent Google from dropping Railway's internal localized server names as DMARC spoofing
    image_cid = make_msgid(domain='iwinback.com')
    
    trend = "⬆️ UP" if avg_7 >= avg_30 else "⬇️ DOWN"
    
    html = f'''
    <div style="margin-bottom: 20px; font-family: sans-serif;">
        <img src="cid:{image_cid[1:-1]}" alt="{title}" style="max-width: 100%; border: 1px solid #ddd; border-radius: 4px;">
        <table style="width: 100%; text-align: center; border-collapse: collapse; margin-top: 5px; background: #f9f9f9; font-size: 14px;">
            <tr>
                <td style="padding: 5px; border: 1px solid #ddd;">30-Day Avg: <b>{avg_30:,.0f}</b></td>
                <td style="padding: 5px; border: 1px solid #ddd;">7-Day Avg: <b>{avg_7:,.0f}</b></td>
                <td style="padding: 5px; border: 1px solid #ddd;">Yesterday: <b>{yesterday_val:,.0f}</b></td>
                <td style="padding: 5px; border: 1px solid #ddd;">Trend: <b>{trend}</b></td>
            </tr>
        </table>
    </div>
    '''
    return html, {'bytes': img_bytes, 'cid': image_cid}

def generate_executive_summary(client_df, client_name, sla_limit, end_date):
    lifecycles = sorted([lc for lc in client_df['lifecycle'].dropna().unique() if str(lc).strip().upper() != 'UNKNOWN'])
    if not lifecycles: return "", None
    
    metrics = [('records', 'New Data', False), ('logins', 'Logins', False), ('conversions', 'Conversions', False), ('total_cost', 'Total Cost', True)]
    
    z_scores_heatmap = []
    text_heatmap = []
    insight_bullets = []
    
    target_weekday = end_date.weekday()
    table_rows = ""
    
    for lc in lifecycles:
        lc_df = client_df[client_df['lifecycle'] == lc].copy()
        lc_df['ops_date'] = pd.to_datetime(lc_df['ops_date'])
        
        date_range = pd.date_range(end_date - timedelta(days=55), end_date)
        daily = lc_df.groupby('ops_date')[['records', 'logins', 'conversions', 'total_cost']].sum().reindex(date_range).fillna(0)
        
        if daily.sum().sum() == 0:
            continue
        
        y_val = daily.iloc[-1]
        l7_val = daily.iloc[-7:].sum()
        w8_avg = daily.sum() / 8.0
        
        dow_dates = [pd.Timestamp(end_date - timedelta(days=7*i)) for i in range(1, 5)]
        dow_4w_vals = daily.reindex(dow_dates).fillna(0).mean()
        
        lc_z = []
        lc_txt = []
        
        for i, (col, label, is_cost) in enumerate(metrics):
            y = y_val[col]
            dow = dow_4w_vals[col]
            l7 = l7_val[col]
            w8 = w8_avg[col]
            daily_avg = daily[col].sum() / 56.0
            
            def get_var(v, b, inv_cost):
                if b == 0:
                    if v == 0: return 0.0, "0%", "black"
                    return 1.0, "+100%", "green" if not inv_cost else "red"
                diff = (v - b) / b
                if abs(diff) < 0.01: return 0.0, "0%", "black"
                
                color = "green" if diff > 0 else "red"
                if inv_cost: color = "red" if diff > 0 else "green"
                sign = "↑" if diff > 0 else "↓"
                return diff, f"{sign} {abs(diff)*100:.0f}%", color
                
            diff_dow, txt_dow, c_dow = get_var(y, dow, is_cost)
            diff_w8, txt_w8, c_w8 = get_var(l7, w8, is_cost)
            
            hm_score = -diff_dow if is_cost else diff_dow
            if abs(hm_score) < 0.01: hm_score = 0.0
            lc_z.append(max(min(hm_score, 0.5), -0.5))
            
            sign_char = "+" if diff_dow > 0 else ""
            txt_cell = f"{sign_char}{diff_dow*100:.0f}%" if abs(diff_dow) >= 0.01 else "0%"
            lc_txt.append(txt_cell)
            
            if hm_score <= -0.15: # 15% worse than benchmark
                if is_cost: insight_bullets.append(f"<li>🚨 <b>{label}</b> for <code>{lc}</code> is trending abnormally high ({txt_dow}).</li>")
                else: insight_bullets.append(f"<li>⚠️ <b>{label}</b> volume for <code>{lc}</code> is down {abs(diff_dow)*100:.0f}% vs historical DOW avg.</li>")
            
            fv_y = f"€{y:,.0f}" if is_cost else f"{y:,.0f}"
            fv_daily_avg = f"€{daily_avg:,.0f}" if is_cost else f"{daily_avg:,.0f}"
            fv_dow = f"€{dow:,.0f}" if is_cost else f"{dow:,.0f}"
            fv_l7 = f"€{l7:,.0f}" if is_cost else f"{l7:,.0f}"
            fv_w8 = f"€{w8:,.0f}" if is_cost else f"{w8:,.0f}"
            
            th_lc = f"<td rowspan='4' style='vertical-align: middle; font-weight: bold; border: 1px solid #ddd; background: #fafbfc;'>{lc}</td>" if i == 0 else ""
            table_rows += f"<tr>{th_lc}<td style='border: 1px solid #ddd; padding: 6px;'><b>{label}</b></td><td style='border: 1px solid #ddd; padding: 6px; text-align: right;'>{fv_y}</td><td style='border: 1px solid #ddd; padding: 6px; text-align: right; color: #586069;'>{fv_daily_avg}</td><td style='border: 1px solid #ddd; padding: 6px; text-align: right;'>{fv_dow} <span style='color:{c_dow}'><b>{txt_dow}</b></span></td><td style='border: 1px solid #ddd; padding: 6px; text-align: right;'>{fv_l7}</td><td style='border: 1px solid #ddd; padding: 6px; text-align: right;'>{fv_w8} <span style='color:{c_w8}'><b>{txt_w8}</b></span></td></tr>"
        
        z_scores_heatmap.append(lc_z)
        text_heatmap.append(lc_txt)
        
    mtd_records = client_df[pd.to_datetime(client_df['ops_date']).dt.month == end_date.month]['records'].sum()
    if sla_limit > 0:
        prorated_sla = (sla_limit / 30.0) * end_date.day 
        pacing_pct = (mtd_records / prorated_sla) if prorated_sla > 0 else 1.0
        if pacing_pct < 1.0: insight_bullets.append(f"<li>⚠️ <b>SLA Pacing:</b> {client_name} is currently tracking at {pacing_pct*100:.0f}% of the contractual monthly minimum.</li>")
        else: insight_bullets.insert(0, f"<li>✅ <b>SLA Pacing:</b> {client_name} is safely tracking over the contractual minimum ({pacing_pct*100:.0f}% pacing).</li>")
            
    if not insight_bullets: insight_bullets.append("<li>✅ <b>Working as expected.</b> All metrics performing near or above 4-week historical baselines.</li>")
        
    ul_insights = "<ul style='padding-left: 20px; line-height: 1.5;'>" + "".join(insight_bullets) + "</ul>"
    
    html = f'''
    <div style="background: #fff; border: 1px solid #e1e4e8; border-radius: 6px; padding: 16px; margin-bottom: 20px;">
        <h3 style="margin-top: 0; color: #24292e; font-size: 16px; border-bottom: 1px solid #eaecef; padding-bottom: 8px;">🤖 Executive Summary & Insights</h3>
        <div style="font-size: 14px; color: #24292e;">{ul_insights}</div>
        
        <table style="width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 13px; color: #24292e;">
            <tr style="background: #f6f8fa; color: #24292e;">
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Lifecycle</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Metric</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">Yesterday</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">Daily Avg.</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">vs 4W DOWAvg</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">Last 7 Days</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">vs 8W WklyAvg</th>
            </tr>
            {table_rows}
        </table>
    </div>
    '''
    return html, None

def generate_morning_briefing():
    """Generates the HTML email and dispatches it via SMTP."""
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    
    if not smtp_host:
        print("❌ Error: SMTP_HOST environment variable not found. Cannot send email.")
        return

    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=55) # 56 days total for 8-week baseline
    chart_start_date = end_date - timedelta(days=29) # 30 days for detailed visual charts
    
    chart_images = []
    
    print(f"📊 Fetching data from {start_date} to {end_date}...")
    df = fetch_data(start_date, end_date)
    slas = fetch_slas()
    
    clients = ['Reliato', 'Limitless', 'Simplicity', 'LeoVegas', 'Offside', 'Powerplay', 'Magico Games/Interspin', 'Rhino']
    
    # Normalize database string quirks to match the target report names natively
    alias_map = {
        'Simplicity Malta Limited': 'Simplicity',
        'LeoVegas Group': 'LeoVegas',
        'Offside Gaming': 'Offside',
        'PowerPlay': 'Powerplay',
        'Magico Games': 'Magico Games/Interspin'
    }
    df['client'] = df['client'].replace(alias_map)
    
    html_body = f'''
    <html>
    <head><style>body {{font-family: Arial, sans-serif; background: #edf2f7; padding: 20px; color: #333; }}</style></head>
    <body>
        <div style="max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
            <h1 style="color: #2c3e50; text-align: center;">📊 D-ROCK Morning Briefing</h1>
            <p style="text-align: center; color: #7f8c8d;">Operational Report for {chart_start_date} to {end_date}</p>
            <div style="background: #fff3cd; color: #856404; border: 1px solid #ffc107; border-radius: 6px; padding: 12px 16px; margin: 0 0 20px 0; font-size: 13px;">
                <b>⏱️ Point-in-Time Snapshot</b> — This report reflects data as ingested at the time of generation ({datetime.now().strftime('%b %d, %Y %H:%M UTC')}). Metrics such as Logins and Conversions may increase in subsequent exports due to delayed attribution from the iWinBack API (up to T+7 days).
            </div>
            <hr style="border: 0; height: 1px; background: #eee; margin-bottom: 30px;">
    '''
    
    for client in clients:
        client_df = df[df['client'] == client].copy()
        
        # Get SLA Limit
        sla_row = slas[slas['client'] == client]
        sla_limit = sla_row['sla_limit'].values[0] if not sla_row.empty else 0
        
        # Generate new Executive Summary (Heatmap, Insights, Toggled Tables)
        exec_html, exec_img = generate_executive_summary(client_df, client, sla_limit, end_date)
        
        if exec_html:
            html_body += f'''
            <h2 style="color: #2980b9; margin-top: 40px; border-bottom: 2px solid #3498db; padding-bottom: 5px;">🏢 {client}</h2>
            {exec_html}
            '''
        else:
            html_body += f'''
            <h2 style="color: #2980b9; margin-top: 40px; border-bottom: 2px solid #3498db; padding-bottom: 5px;">🏢 {client}</h2>
            <div style="background: #e2e3e5; color: #383d41; padding: 10px; border-radius: 5px; margin-bottom: 20px;"><b>ℹ️ No active campaigns for {client} in this period.</b></div>
            '''
            continue # Skip rendering detailed charts if no data
            
    html_body += '''
        </div>
    </body>
    </html>
    '''
    
    print("🚀 Compiling HTML payload and initiating SMTP connection...")
    
    msg = EmailMessage()
    msg['Subject'] = f"📊 Automated D-ROCK Briefing - {datetime.now().strftime('%b %d, %Y')}"
    msg['From'] = smtp_user
    
    receivers = ["dani.fabregas@iwinback.com"]
    msg['To'] = ", ".join(receivers)
    
    msg.set_content("Please enable HTML to view this report.")
    msg.add_alternative(html_body, subtype='html')
    
    # Securely append all generated PNGs as legitimate CID elements rather than string blobs
    html_part = msg.get_payload()[1]
    for img in chart_images:
        html_part.add_related(img['bytes'], maintype='image', subtype='png', cid=img['cid'])
    
    import socket
    
    # Temporarily monkey-patch socket mapping to force IPv4 routing
    # This bypasses Railway Docker IPv6 dead-ends (Errno 101)
    old_getaddrinfo = socket.getaddrinfo
    def ipv4_getaddrinfo(*args, **kwargs):
        responses = old_getaddrinfo(*args, **kwargs)
        return [r for r in responses if r[0] == socket.AF_INET]
    
    socket.getaddrinfo = ipv4_getaddrinfo
    
    try:
        with smtplib.SMTP(smtp_host, int(smtp_port)) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        print("✅ Live Sample Email successfully dispatched to dani.fabregas@iwinback.com!")
    finally:
        socket.getaddrinfo = old_getaddrinfo # Ensure global safety restore

if __name__ == "__main__":
    generate_morning_briefing()
