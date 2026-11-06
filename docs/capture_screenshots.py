"""
Captures high-resolution screenshots of the Streamlit dashboard for academic report.
"""
from playwright.sync_api import sync_playwright
import time
from pathlib import Path

ASSETS_DIR = Path("docs/assets")
ASSETS_DIR.mkdir(parents=True, exist_ok=True)

with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=True)
    page = browser.new_page(viewport={"width": 1600, "height": 1150})
    page.goto("http://localhost:8501", wait_until="networkidle", timeout=20000)
    page.wait_for_timeout(4500)

    # 1. Full page overview
    page.screenshot(path=str(ASSETS_DIR / "dashboard_overview.png"), full_page=True)
    print("Saved: docs/assets/dashboard_overview.png")

    # 2. Viewport showing Title, KPI cards, and dual-trace time-series charts
    page.screenshot(path=str(ASSETS_DIR / "dashboard_kpi_charts.png"))
    print("Saved: docs/assets/dashboard_kpi_charts.png")

    # 3. Click Tab 2: Anomaly Alerts Log
    tab2 = page.get_by_role("tab", name="🚨 Anomaly Alerts Log")
    if tab2.count() > 0:
        tab2.click()
        page.wait_for_timeout(2000)
        page.screenshot(path=str(ASSETS_DIR / "dashboard_anomaly_log.png"), full_page=True)
        print("Saved: docs/assets/dashboard_anomaly_log.png")
    else:
        print("Tab 2 not found by role name.")

    # 4. Click Tab 3: Server Fleet Summary
    tab3 = page.get_by_role("tab", name="🖥️ Server Fleet Summary")
    if tab3.count() > 0:
        tab3.click()
        page.wait_for_timeout(2000)
        page.screenshot(path=str(ASSETS_DIR / "dashboard_fleet_summary.png"), full_page=True)
        print("Saved: docs/assets/dashboard_fleet_summary.png")
    else:
        print("Tab 3 not found by role name.")

    browser.close()
    print("All screenshots successfully captured!")
