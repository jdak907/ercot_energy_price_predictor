"""
EEPP Phase 2
------------
Downloads DAM CPC/SPP (yesterday vs. today), pulls RTM via ERCOT API,
builds DART (DAM–RTM) comparisons, saves combined plots and an Excel bundle.
"""
from __future__ import annotations
import logging, os
from datetime import datetime, timedelta
from io import BytesIO
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import pandas as pd
import requests, zipfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

LOG_FILE = "eepp_ph_2.log"
PRODUCTION_DIR = "production"
ARCHIVE_DIR = os.path.join(PRODUCTION_DIR, "archive")
ICONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "icons")
DEFAULT_LOGO = os.path.normpath(os.path.join(ICONS_DIR, "logo.png"))
HEADLESS = True
URL_DAMCPC = "https://www.ercot.com/mp/data-products/data-product-details?id=NP4-188-CD"
URL_DAMSPP = "https://www.ercot.com/mp/data-products/data-product-details?id=NP4-190-CD"
SLACK_TOKEN = os.environ.get("SLACK_TOKEN"); SLACK_CHANNEL = "#eepp"
API_USER = os.environ.get("ERCOT_API_USERNAME")
API_PASS = os.environ.get("ERCOT_API_PASSWORD")
API_KEY  = os.environ.get("ERCOT_API_PRIMARY_KEY")
TOKEN_RESOURCE = ("https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/"
                  "B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    filename=LOG_FILE, filemode="a")
log = logging.getLogger("eepp_phase_2")
log.info("****** BEGIN - EEPP Phase 2 script.")

def ensure_dirs():
    os.makedirs(PRODUCTION_DIR, exist_ok=True); os.makedirs(ARCHIVE_DIR, exist_ok=True)

def is_file_older_than_today(p:str)->bool:
    from datetime import datetime as dt
    return dt.fromtimestamp(os.path.getctime(p)).date() < dt.now().date()

def archive_old_files():
    for fn in os.listdir(PRODUCTION_DIR):
        src = os.path.join(PRODUCTION_DIR, fn); dst = os.path.join(ARCHIVE_DIR, fn)
        if os.path.isfile(src) and is_file_older_than_today(src):
            if os.path.exists(dst): os.remove(dst)
            from shutil import move; move(src, dst)
    log.info("Moved old files to archive.")

def send_slack_notification(message:str, files=None):
    if not SLACK_TOKEN: log.info("SLACK_TOKEN not set; skipping Slack."); return
    client = WebClient(token=SLACK_TOKEN)
    try:
        resp = client.chat_postMessage(channel=SLACK_CHANNEL, text=message, mrkdwn=True); ts = resp["ts"]
        for fp in files or []:
            with open(fp, "rb") as fh:
                client.files_upload_v2(channel=resp["channel"], thread_ts=ts, file=fh, title=os.path.basename(fp))
    except SlackApiError as e:
        log.error("Slack error: %s", e.response.get("error", "unknown"))

def get_download_link(driver, url:str, link_text="zip", link_index=0)->str:
    log.info("Fetching download link from %s", url); driver.get(url)
    links = WebDriverWait(driver, 20).until(
        EC.presence_of_all_elements_located((By.LINK_TEXT, link_text))
    )
    href = links[link_index].get_attribute("href"); log.info("Fetched: %s", href); return href

def download_and_extract_zip(url:str, extract_to:str=".")->str:
    log.info("Downloading/extracting ZIP: %s", url)
    r = requests.get(url, timeout=60); r.raise_for_status()
    with zipfile.ZipFile(BytesIO(r.content)) as zf:
        zf.extractall(extract_to); names = zf.namelist()
    return os.path.join(extract_to, names[0])

def process_file(driver, page_url:str, link_index:int, progress:tqdm, download_dir:str=".")->str:
    href = get_download_link(driver, page_url, "zip", link_index)
    if not href.startswith("http"): href = "https://www.ercot.com"+href
    out = download_and_extract_zip(href, extract_to=download_dir); progress.update(1); return out

def get_api_token()->str:
    data = {"username": API_USER, "password": API_PASS, "grant_type": "password",
            "scope": "openid fec253ea-0d06-4272-a5e6-b478baeecd70 offline_access",
            "client_id": "fec253ea-0d06-4272-a5e6-b478baeecd70", "response_type": "id_token"}
    r = requests.post(TOKEN_RESOURCE, data=data, timeout=60); r.raise_for_status()
    token = r.json().get("id_token"); 
    if not token: raise RuntimeError("ERCOT API token missing"); 
    log.info("ERCOT API auth OK."); return token

def api_headers(token:str)->dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json", "Ocp-Apim-Subscription-Key": API_KEY or ""}

def get_api_rtm_spp_yesterday(prod_dir:str):
    if not (API_USER and API_PASS and API_KEY):
        log.error("Missing ERCOT API credentials"); return None
    token = get_api_token(); yday = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
    url = "https://api.ercot.com/api/public-reports/np6-905-cd/spp_node_zone_hub"
    params = {"deliveryDateFrom": yday, "deliveryDateTo": yday, "deliveryHourFrom": 1, "deliveryHourTo": 24,
              "deliveryIntervalFrom": 1, "deliveryIntervalTo": 4, "settlementPointType": "LZ", "page": 1, "size": 100}
    rows = []; 
    while True:
        r = requests.get(url, headers=api_headers(token), params=params, timeout=60); r.raise_for_status()
        payload = r.json(); data = payload.get("data", []); rows += data
        if len(data) < params["size"]: break
        params["page"] += 1
    if not rows: return None
    fields = payload.get("fields"); cols = [f["name"] for f in fields] if fields else None
    df = pd.DataFrame(rows, columns=cols); out = os.path.join(prod_dir, f"df_rtm_spp_yesterday_{yday}.csv")
    df.to_csv(out, index=False); log.info("Saved RTM SPP yesterday to %s", out); return out

def get_api_dam_spp_yesterday(prod_dir:str):
    if not (API_USER and API_PASS and API_KEY):
        log.error("Missing ERCOT API credentials"); return None
    token = get_api_token(); yday = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
    url = "https://api.ercot.com/api/public-reports/np4-190-cd/dam_stlmnt_pnt_prices"
    params = {"deliveryDateFrom": yday, "deliveryDateTo": yday, "page": 1, "size": 100}
    rows = []
    while True:
        r = requests.get(url, headers=api_headers(token), params=params, timeout=60); r.raise_for_status()
        payload = r.json(); data = payload.get("data", []); rows += data
        if len(data) < params["size"]: break
        params["page"] += 1
    if not rows: return None
    df = pd.DataFrame(rows); out = os.path.join(prod_dir, f"df_dam_spp_yesterday_{yday}.csv")
    df.to_csv(out, index=False); log.info("Saved DAM SPP yesterday (API) to %s", out); return out

def plot_dam(ax, df_today, df_yday, sp, title):
    t = df_today[df_today["SettlementPoint"]==sp]; y = df_yday[df_yday["SettlementPoint"]==sp]
    ax.plot(t["HourEnding"], t["SettlementPointPrice"], label=f"Today’s DAM @ {sp} for Tomorrow")
    ax.plot(y["HourEnding"], y["SettlementPointPrice"], label=f"Yesterday’s DAM @ {sp} for Today")
    ax.set_xlabel("Hour Ending"); ax.set_ylabel("SettlementPointPrice"); ax.set_title(title, fontsize=16)
    ax.legend(loc="best"); ax.grid(True)

def plot_anc(ax, df_today, df_yday, atype, title):
    t = df_today[df_today["AncillaryType"]==atype]; y = df_yday[df_yday["AncillaryType"]==atype]
    ax.plot(t["HourEnding"], t["MCPC"], label=f"Today’s {atype} for Tomorrow")
    ax.plot(y["HourEnding"], y["MCPC"], label=f"Yesterday’s {atype} for Today")
    ax.set_xlabel("Hour Ending"); ax.set_ylabel("MCPC"); ax.set_title(title, fontsize=16)
    ax.legend(loc="best"); ax.grid(True)

def plot_dart(ax, dam_yday, rtm_yday, sp, title):
    dam = dam_yday[dam_yday["SettlementPoint"]==sp].copy()
    rtm = rtm_yday[rtm_yday["settlementPoint"]==sp].copy()
    rtm_hourly = rtm.groupby(["datetime"], as_index=False)["settlementPointPrice"].mean()
    dam = dam.set_index("datetime").reindex(rtm_hourly["datetime"]).reset_index()
    spread = dam["SettlementPointPrice"].values - rtm_hourly["settlementPointPrice"].values
    ax.plot(dam["datetime"], dam["SettlementPointPrice"], label=f"DAM - {sp}")
    ax.plot(rtm_hourly["datetime"], rtm_hourly["settlementPointPrice"], label=f"RTM - {sp}")
    ax2 = ax.twinx(); ax2.bar(dam["datetime"], spread, alpha=0.3, label="DART Spread")
    ax.set_xlabel("Hour Ending"); ax.set_ylabel("SettlementPointPrice"); ax2.set_ylabel("DART Spread")
    ax.set_title(title, fontsize=16); ax.legend(loc="upper left"); ax2.legend(loc="upper right"); ax.grid(True)

def plot_all(df_dam_spp_today, df_dam_spp_yday, df_dam_cpc_today, df_dam_cpc_yday, df_rtm_spp_yday, logo_path)->str:
    fig, axs = plt.subplots(3, 2, figsize=(20, 30))
    plot_dam(axs[0,0], df_dam_spp_today, df_dam_spp_yday, "LZ_HOUSTON", "DAM LZ HOUSTON: Yesterday vs. Today")
    plot_dam(axs[0,1], df_dam_spp_today, df_dam_spp_yday, "LZ_NORTH",   "DAM LZ NORTH: Yesterday vs. Today")
    plot_anc(axs[1,0], df_dam_cpc_today, df_dam_cpc_yday, "ECRS", "ERCOT DAM Clearing Prices for Capacity")
    plot_anc(axs[1,1], df_dam_cpc_today, df_dam_cpc_yday, "RRS",  "ERCOT RRS Yesterday vs. Today")
    plot_anc(axs[2,0], df_dam_cpc_today, df_dam_cpc_yday, "NSPIN","ERCOT NSPIN Yesterday vs. Today")
    if not df_rtm_spp_yday.empty: plot_dart(axs[2,1], df_dam_spp_yday, df_rtm_spp_yday, "LZ_NORTH", "DART Spread - LZ_NORTH")
    else: fig.delaxes(axs[2,1])
    fig.text(0.95, 0.04, datetime.now().isoformat(), ha="right", fontsize=12)
    try:
        if os.path.exists(logo_path):
            logo = mpimg.imread(logo_path); fig.figimage(logo, 50, 50, zorder=10, alpha=0.5)
    except Exception: pass
    out = os.path.join(PRODUCTION_DIR, f"combined_plots_{datetime.now().strftime('%Y-%m-%d_T%H_%M_%S')}.png")
    plt.savefig(out, bbox_inches="tight"); plt.close(fig); return out

def main():
    ensure_dirs(); archive_old_files()
    total = 6; progress = tqdm(total=total, desc="Phase 2: Processing Files")
    service = Service(ChromeDriverManager().install())
    opts = webdriver.ChromeOptions(); opts.add_argument("--headless=new")
    driver = webdriver.Chrome(service=service, options=opts)
    try:
        dam_cpc_today_fp = process_file(driver, URL_DAMCPC, link_index=0, progress=progress, download_dir=PRODUCTION_DIR)
        dam_cpc_yday_fp  = process_file(driver, URL_DAMCPC, link_index=2, progress=progress, download_dir=PRODUCTION_DIR)
        dam_spp_today_fp = process_file(driver, URL_DAMSPP, link_index=0, progress=progress, download_dir=PRODUCTION_DIR)
        dam_spp_yday_fp  = process_file(driver, URL_DAMSPP, link_index=2, progress=progress, download_dir=PRODUCTION_DIR)
    finally:
        driver.quit()
    df_rtm_spp_yday_filename = get_api_rtm_spp_yesterday(PRODUCTION_DIR); progress.update(1)
    df_api_dam_spp_yday_filename = get_api_dam_spp_yesterday(PRODUCTION_DIR)
    df_dam_cpc_today = pd.read_csv(dam_cpc_today_fp); progress.update(1)
    df_dam_cpc_yday  = pd.read_csv(dam_cpc_yday_fp);  progress.update(1)
    df_dam_spp_today = pd.read_csv(dam_spp_today_fp); progress.update(1)
    df_dam_spp_yday  = pd.read_csv(dam_spp_yday_fp);  progress.update(1)
    if df_rtm_spp_yday_filename: df_rtm_spp_yday = pd.read_csv(df_rtm_spp_yday_filename); 
    else: df_rtm_spp_yday = pd.DataFrame()
    if df_api_dam_spp_yday_filename: df_api_dam_spp_yday = pd.read_csv(df_api_dam_spp_yday_filename)
    else: df_api_dam_spp_yday = pd.DataFrame()
    for df in [df_dam_cpc_today, df_dam_cpc_yday, df_dam_spp_today, df_dam_spp_yday]:
        df["DeliveryDate"] = pd.to_datetime(df["DeliveryDate"])
        df["HourEnding"] = df["HourEnding"].astype(str).str.split(":").str[0].astype(int)
        df["datetime"] = pd.to_datetime(df["DeliveryDate"].astype(str) + " " + (df["HourEnding"] - 1).astype(str) + ":00")
    if not df_rtm_spp_yday.empty:
        df_rtm_spp_yday["deliveryDate"] = pd.to_datetime(df_rtm_spp_yday["deliveryDate"])
        df_rtm_spp_yday["datetime"] = pd.to_datetime(df_rtm_spp_yday["deliveryDate"].astype(str) + " " +
                                                     (df_rtm_spp_yday["deliveryHour"] - 1).astype(str) + ":00")
    excel_path = os.path.join(PRODUCTION_DIR, f"ercot_data_{datetime.now().strftime('%Y-%m-%d_T%H_%M_%S')}.xlsx")
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        df_dam_cpc_today.to_excel(writer, sheet_name="DAM_CPC_Today", index=False)
        df_dam_cpc_yday.to_excel(writer,  sheet_name="DAM_CPC_Yesterday", index=False)
        df_dam_spp_today.to_excel(writer, sheet_name="DAM_SPP_Today", index=False)
        df_dam_spp_yday.to_excel(writer,  sheet_name="DAM_SPP_Yesterday", index=False)
        if not df_rtm_spp_yday.empty: df_rtm_spp_yday.to_excel(writer, sheet_name="RTM_SPP_Yesterday", index=False)
        if not df_api_dam_spp_yday.empty: df_api_dam_spp_yday.to_excel(writer, sheet_name="DAM_SPP_Yesterday_API", index=False)
    plot_path = plot_all(df_dam_spp_today, df_dam_spp_yday, df_dam_cpc_today, df_dam_cpc_yday, df_rtm_spp_yday, DEFAULT_LOGO)
    from os.path import basename
    send_slack_notification(f"*DAM/RTM Reports*\\nNew ERCOT plots & workbook generated.\\n\\n{basename(plot_path)}\\n{basename(excel_path)}",
                            files=[plot_path])
    progress.close(); log.info("END - EEPP Phase 2 script. ******")

if __name__ == "__main__":
    main()
