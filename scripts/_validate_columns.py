#!/usr/bin/env python3
"""Validate header/row alignment for every modified Android tab by running the
real build + write functions against the live DB with a mocked Google Sheet.
Asserts: every data row's width == its header row's width. Also proves the new
p0p1 SQL is valid (build_* functions execute it)."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import update_meta_dashboard as U


class FakeWS:
    _ids = iter(range(1000, 9999))
    def __init__(self, title, sh):
        self.title = title
        self.id = next(FakeWS._ids)
        self.spreadsheet = sh
        self.captured = None
    def update(self, values=None, range_name=None, value_input_option=None):
        # gspread also supports positional update(range, values); handle both
        if values is None and range_name is not None:
            values = range_name
        self.captured = values
    def format(self, *a, **k): pass
    def clear(self): pass


class FakeSheet:
    def __init__(self):
        self.url = "fake"; self.id = "fake"; self.worksheets_made = []
    def worksheet(self, name):
        raise Exception("not found")  # force the writers' create path
    def del_worksheet(self, ws): pass
    def add_worksheet(self, title, rows=100, cols=26):
        ws = FakeWS(title, self); self.worksheets_made.append(ws); return ws
    def batch_update(self, body): pass


def check(tab, captured):
    """Find the header row (contains 'Signups' or 'P0P1 %'), assert all data
    rows match its width."""
    if not captured:
        print(f"  {tab:28s} NO DATA CAPTURED"); return False
    hdr_idx = None
    for i, row in enumerate(captured[:3]):
        if row and ("Signups" in row or "P0P1 %" in row):
            hdr_idx = i; break
    if hdr_idx is None:
        print(f"  {tab:28s} header row not found"); return False
    H = len(captured[hdr_idx])
    bad = []
    data_lens = set()
    for i, row in enumerate(captured):
        L = len(row)
        if L > H:
            bad.append((i, L))            # row wider than header == misalignment
        if L > H // 2:                    # "full" data/header rows
            data_lens.add(L)
    has_p0p1 = "P0P1 %" in captured[hdr_idx]
    ok = (not bad) and (data_lens == {H})
    flag = "OK " if ok else "FAIL"
    print(f"  [{flag}] {tab:26s} header={H}  P0P1%={'Y' if has_p0p1 else 'N'}  data_row_widths={sorted(data_lens)}"
          + (f"  OVERWIDE_ROWS={bad[:3]}" if bad else ""))
    return ok


def main():
    conn = U.db_conn()
    print("Building data (executes the new p0p1 SQL)...")
    ad_rows   = U.build_ad_data(conn)
    camp_rows = U.build_campaign_data(conn)
    adx_rows  = U.build_ad_x_date_data(ad_rows)
    day_rows  = U.build_day_level_data(conn, ad_rows)
    cday_rows = U.build_campaign_day_level_data(conn)
    hourly    = U.build_hourly_performance_data(conn, days=7)
    test_rows = U.build_test_creatives_data(ad_rows, adx_rows)
    print(f"  ad={len(ad_rows)} camp={len(camp_rows)} adx={len(adx_rows)} day={len(day_rows)} "
          f"cday={len(cday_rows)} hourly={len(hourly)} test={len(test_rows)}")

    # Spot-check p0p1 presence + sample values
    sample = next((r for r in ad_rows if r.get("signups")), None)
    if sample:
        print(f"  sample ad: signups={sample.get('signups')} p0p1_signups={sample.get('p0p1_signups')} "
              f"p0p1_pct={U._p0p1_pct(sample)}")
    conn.close()

    results = []
    sh = FakeSheet()
    U.write_ad_level_sheet(sh, ad_rows);                  results.append(check("Ad Level", sh.worksheets_made[-1].captured))
    U.write_campaign_level_sheet(sh, camp_rows);          results.append(check("Campaign Level", sh.worksheets_made[-1].captured))
    U.write_ad_x_date_sheet(sh, adx_rows);                results.append(check("Ad × Date", sh.worksheets_made[-1].captured))
    U.write_day_level_sheet(sh, day_rows);                results.append(check("Day Level — Ads", sh.worksheets_made[-1].captured))
    U.write_campaign_day_level_sheet(sh, cday_rows);      results.append(check("Day Level — Campaigns", sh.worksheets_made[-1].captured))
    U.write_inefficient_sheet(sh, ad_rows);               results.append(check("Action Required", sh.worksheets_made[-1].captured))
    U.write_test_creatives_sheet(sh, test_rows);          results.append(check("Test Creatives", sh.worksheets_made[-1].captured))
    U.write_hourly_performance_sheet(sh, hourly, days=7); results.append(check("Hourly Performance", sh.worksheets_made[-1].captured))
    U.write_search_sheet(sh);                             results.append(check("Search", sh.worksheets_made[-1].captured))

    print()
    if all(results):
        print("ALL TABS ALIGNED ✓")
    else:
        print("MISALIGNMENT DETECTED ✗"); sys.exit(1)


if __name__ == "__main__":
    main()
