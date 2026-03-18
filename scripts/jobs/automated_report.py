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
    query = f"""
        SELECT 
            ops_date, ops_client as client, extracted_lifecycle as lifecycle,
            SUM(records) as records, SUM(kpi2_logins) as logins, SUM(conversions) as conversions
        FROM ops_telemarketing_data
        WHERE ops_date >= '{start_date}' AND ops_date <= '{end_date}'
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
    start_date = end_date - timedelta(days=29) # 30 days total
    
    chart_images = []
    
    print(f"📊 Fetching data from {start_date} to {end_date}...")
    df = fetch_data(start_date, end_date)
    slas = fetch_slas()
    
    clients = ['Reliato', 'Limitless', 'Simplicity', 'LeoVegas', 'Offside', 'Powerplay', 'Magico Games/Interspin', 'Rhino']
    lifecycles = ['WB', 'RND']
    
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
            <p style="text-align: center; color: #7f8c8d;">Operational Report for {start_date} to {end_date}</p>
            <hr style="border: 0; height: 1px; background: #eee; margin-bottom: 30px;">
    '''
    
    for client in clients:
        client_df = df[df['client'] == client].copy()
        
        # SLA Calculation
        total_30_volume = client_df['records'].sum()
        sla_row = slas[slas['client'] == client]
        sla_limit = sla_row['sla_limit'].values[0] if not sla_row.empty else 0
        
        if sla_limit > 0:
            if total_30_volume >= sla_limit:
                sla_html = f'<div style="background: #d4edda; color: #155724; padding: 10px; border-radius: 5px; margin-bottom: 20px;"><b>✅ SLA OK:</b> {client} hit {total_30_volume:,.0f} New Data vs SLA limit of {sla_limit:,.0f}</div>'
            else:
                sla_html = f'<div style="background: #f8d7da; color: #721c24; padding: 10px; border-radius: 5px; margin-bottom: 20px;"><b>⚠️ SLA MISSED:</b> {client} hit {total_30_volume:,.0f} New Data vs SLA limit of {sla_limit:,.0f}</div>'
        else:
            sla_html = f'<div style="background: #e2e3e5; color: #383d41; padding: 10px; border-radius: 5px; margin-bottom: 20px;"><b>ℹ️ No Contractual SLA configured for {client}</b> (Volume: {total_30_volume:,.0f})</div>'

        html_body += f'''
        <h2 style="color: #2980b9; margin-top: 40px; border-bottom: 2px solid #3498db; padding-bottom: 5px;">🏢 {client}</h2>
        {sla_html}
        '''
        
        for lc in lifecycles:
            lc_df = client_df[client_df['lifecycle'] == lc].copy()
            if lc_df.empty:
                continue
                
            # Date padding
            lc_df['ops_date'] = pd.to_datetime(lc_df['ops_date'])
            date_range = pd.date_range(start=start_date, end=end_date)
            lc_df.set_index('ops_date', inplace=True)
            lc_df = lc_df.reindex(date_range).fillna(0).reset_index()
            lc_df.rename(columns={'index': 'ops_date'}, inplace=True)
            
            html_body += f'<h3 style="color: #34495e; background: #ecf0f1; padding: 8px; border-radius: 4px;">Lifecycle: {lc}</h3>'
            
            # 1. Volume
            vol_html, vol_img = create_chart_base64(lc_df, 'ops_date', 'records', f"{client} {lc} - Volume", "#3498db")
            html_body += vol_html
            chart_images.append(vol_img)
            
            # 2. Logins
            log_html, log_img = create_chart_base64(lc_df, 'ops_date', 'logins', f"{client} {lc} - Logins", "#2ecc71")
            html_body += log_html
            chart_images.append(log_img)
            
            # 3. Conversions
            con_html, con_img = create_chart_base64(lc_df, 'ops_date', 'conversions', f"{client} {lc} - Conversions", "#9b59b6")
            html_body += con_html
            chart_images.append(con_img)
            
    html_body += '''
        </div>
    </body>
    </html>
    '''
    
    print("🚀 Compiling HTML payload and initiating SMTP connection...")
    
    msg = EmailMessage()
    msg['Subject'] = f"📊 Automated D-ROCK Briefing - {datetime.now().strftime('%b %d, %Y')}"
    msg['From'] = smtp_user
    
    receivers = ["dani.fabregas@iwinback.com", "julija.stanojevic@callsu.net"]
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
