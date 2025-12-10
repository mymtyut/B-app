import streamlit as st
import pandas as pd
import jpholiday
import math
import datetime
import calendar
import json
import time
from dateutil.relativedelta import relativedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==========================================
# 1. 関数定義エリア
# ==========================================

# --- GSpread 接続 ---
@st.cache_resource
def get_gspread_client():
    key_dict = st.secrets["gcp_service_account"]
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    client = gspread.authorize(creds)
    return client

@st.cache_resource(ttl=600)
def get_spreadsheet():
    client = get_gspread_client()
    sheet_url = st.secrets["spreadsheet"]["url"]
    return client.open_by_url(sheet_url)

# --- データ読み書き ---
def load_data_from_sheet(worksheet_name, default_df=None):
    sh = get_spreadsheet()
    try:
        worksheet = sh.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        if not data:
            return default_df if default_df is not None else pd.DataFrame()
        return pd.DataFrame(data)
    except gspread.WorksheetNotFound:
        if default_df is not None:
            time.sleep(1)
            worksheet = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
            save_data_to_sheet(worksheet_name, default_df)
            return default_df
        return pd.DataFrame()

def save_data_to_sheet(worksheet_name, df):
    sh = get_spreadsheet()
    try:
        worksheet = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        time.sleep(1)
        worksheet = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
    
    headers = df.columns.values.tolist()
    data_list = df.values.tolist()
    all_values = [headers] + data_list
    
    try:
        worksheet.resize(rows=max(len(all_values)+10, 100), cols=max(len(headers), 5))
    except:
        pass
    
    clean_params = []
    for row in all_values:
        clean_row = []
        for cell in row:
            if isinstance(cell, list):
                cell = cell[0] if len(cell) > 0 else ""
            if isinstance(cell, (datetime.date, datetime.datetime, datetime.time)):
                clean_row.append(str(cell))
            elif pd.isna(cell):
                clean_row.append("")
            else:
                clean_row.append(str(cell))
        clean_params.append(clean_row)
    
    worksheet.clear()
    worksheet.update(range_name='A1', values=clean_params)

# --- 設定値のJSON変換保存 ---
def load_settings_from_sheet():
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet("settings")
        val = ws.acell('A1').value
        if val:
            settings = json.loads(val)
            keys_to_date = ["opening_date", "wage_start", "transport_start", "lunch_start"]
            keys_to_time = ["open_time", "close_time"]
            
            for k in keys_to_date:
                if k in settings and settings[k]:
                    settings[k] = datetime.datetime.strptime(settings[k], "%Y-%m-%d").date()
            for k in keys_to_time:
                if k in settings and settings[k]:
                    settings[k] = datetime.datetime.strptime(settings[k], "%H:%M:%S").time()
            
            for hist_key in ["wage_history", "transport_history", "lunch_history", "capacity_history"]:
                if hist_key in settings:
                    for item in settings[hist_key]:
                        if item.get("start"):
                            item["start"] = datetime.datetime.strptime(item["start"], "%Y-%m-%d").date()
                        if item.get("end"):
                            item["end"] = datetime.datetime.strptime(item["end"], "%Y-%m-%d").date()
                        else:
                            item["end"] = None

            defaults = _get_default_settings_obj()
            for k, v in defaults.items():
                if k not in settings: settings[k] = v
            return settings
    except (gspread.WorksheetNotFound, json.JSONDecodeError, TypeError):
        pass
    return _get_default_settings_obj()

def save_settings_to_sheet(settings_dict):
    s_save = settings_dict.copy()
    for k, v in s_save.items():
        if isinstance(v, (datetime.date, datetime.time)):
            fmt = "%H:%M:%S" if isinstance(v, datetime.time) else "%Y-%m-%d"
            s_save[k] = v.strftime(fmt)
    
    for hist_key in ["wage_history", "transport_history", "lunch_history", "capacity_history"]:
        if hist_key in s_save:
            new_list = []
            for item in s_save[hist_key]:
                new_item = item.copy()
                if isinstance(new_item.get("start"), datetime.date):
                    new_item["start"] = new_item["start"].strftime("%Y-%m-%d")
                if isinstance(new_item.get("end"), datetime.date):
                    new_item["end"] = new_item["end"].strftime("%Y-%m-%d")
                else:
                    new_item["end"] = "" 
                new_list.append(new_item)
            s_save[hist_key] = new_list

    json_str = json.dumps(s_save, ensure_ascii=False)
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet("settings")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="settings", rows=10, cols=10)
    ws.update_acell('A1', json_str)

# --- 共通定数・初期値 ---
DEFAULT_SETTINGS = {
    "facility_name": "就労支援センター 未来",
    "opening_date": "2024-11-01",
    "capacity": 20,
    "open_time": "09:00:00",
    "close_time": "17:00:00",
    "fulltime_hours": 40.0,
    "service_ratio": 6.0, 
    "closed_days": ["土", "日"],
    "close_on_holiday": True,
    "wage_history": [],
    "transport_history": [],
    "lunch_history": [],
    "capacity_history": [{"start": "2024-11-01", "count": 20}],
    "add_ons": [] 
}

RATIO_MAP = {6.0: "6:1", 7.5: "7.5:1", 10.0: "10:1"}
JP_DAYS = ["月","火","水","木","金","土","日"]

def _get_default_settings_obj():
    s = DEFAULT_SETTINGS.copy()
    for k in ["opening_date"]:
        if isinstance(s.get(k), str): s[k] = datetime.datetime.strptime(s[k], "%Y-%m-%d").date()
    for k in ["open_time", "close_time"]:
        if isinstance(s.get(k), str): s[k] = datetime.datetime.strptime(s[k], "%H:%M:%S").time()
    if isinstance(s["capacity_history"][0]["start"], str):
        s["capacity_history"][0]["start"] = datetime.datetime.strptime(s["capacity_history"][0]["start"], "%Y-%m-%d").date()
    return s

# --- 計算・判定ヘルパー関数 ---
def ceil_decimal_1(value):
    return math.ceil(value * 10) / 10

def is_addon_active(target_date, history_list):
    if not history_list: return False
    t = target_date
    for period in history_list:
        start = period.get("start")
        end = period.get("end")
        if start is None: continue 
        if end is None: 
            if t >= start: return True
        else: 
            if start <= t <= end: return True
    return False

def get_capacity_at_date(target_date, history_list):
    if not history_list: return 20
    sorted_hist = sorted(history_list, key=lambda x: x['start'])
    current_cap = 20
    for item in sorted_hist:
        if item['start'] <= target_date:
            current_cap = item['count']
        else:
            break
    return int(current_cap)

def safe_to_date(val):
    if pd.isnull(val): return None
    s_val = str(val).strip()
    s_val = s_val.replace("['", "").replace("']", "").replace('["', "").replace('"]', "").replace("'", "").replace('"', "")
    if s_val == "": return None
    try:
        if isinstance(val, (datetime.date, datetime.datetime)):
            return val.date() if isinstance(val, datetime.datetime) else val
        return pd.to_datetime(s_val).date()
    except:
        return None

def is_special_holiday_recurring(target_date, holiday_df):
    t_md = (target_date.month, target_date.day)
    for _, row in holiday_df.iterrows():
        try:
            s_md = (int(row["開始月"]), int(row["開始日"]))
            e_md = (int(row["終了月"]), int(row["終了日"]))
            if s_md <= e_md:
                if s_md <= t_md <= e_md: return True, row["名称"]
            else:
                if t_md >= s_md or t_md <= e_md: return True, row["名称"]
        except ValueError: continue
    return False, ""

def get_active_staff_df(original_df, settings, target_date_obj=None):
    df = original_df.copy()
    df["入社日"] = df["入社日"].apply(safe_to_date)
    df["退職日"] = df["退職日"].apply(safe_to_date)

    if target_date_obj:
        last_day = calendar.monthrange(target_date_obj.year, target_date_obj.month)[1]
        month_end = datetime.date(target_date_obj.year, target_date_obj.month, last_day)
        
        active_mask = []
        for _, row in df.iterrows():
            hire_date = row["入社日"]
            resign_date = row["退職日"]
            is_hired = True
            if pd.notnull(hire_date) and hire_date > month_end: is_hired = False
            is_resigned = False
            if pd.notnull(resign_date) and resign_date < target_date_obj: is_resigned = True
            active_mask.append(is_hired and not is_resigned)
        df = df[active_mask]

        exclude_targets = []
        wage_active = is_addon_active(target_date_obj, settings.get("wage_history", []))
        lunch_active = is_addon_active(target_date_obj, settings.get("lunch_history", []))
        trans_active = is_addon_active(target_date_obj, settings.get("transport_history", []))
        
        if not wage_active: exclude_targets.append("目標工賃達成指導員")
        if not lunch_active: exclude_targets.append("調理員")
        if not trans_active: exclude_targets.append("運転手")
        
        if exclude_targets:
            df = df[~df["職種(主)"].isin(exclude_targets)]
        
    return df

# 【重要修正】平均利用人数の計算ロジック（4段階）
def calculate_average_users_detail(target_date, opening_date, capacity_history, records_df):
    explanation = { "rule_name": "", "period_start": "", "period_end": "", "details_df": None, "formula": "", "result": 0.0 }
    
    # 0. 基本情報の整理
    target_ym = target_date.replace(day=1)
    open_ym = opening_date.replace(day=1)
    
    # 経過月数（0ヶ月目=開所月）
    diff = relativedelta(target_ym, open_ym)
    months_passed = diff.years * 12 + diff.months
    
    # 定員の取得
    current_capacity = get_capacity_at_date(target_date, capacity_history)
    
    # --- ケース1: 開所6ヶ月間 (months_passed: 0~5) ---
    if months_passed < 6:
        explanation["rule_name"] = f"【新規開所特例】開所6ヶ月間 (定員{current_capacity}名)"
        explanation["formula"] = f"定員 {current_capacity}人 × 90% ＝ 平均利用人数 {ceil_decimal_1(current_capacity * 0.9)}人"
        explanation["result"] = ceil_decimal_1(current_capacity * 0.9)
        return explanation

    # 実績データの準備
    if records_df.empty:
        explanation["rule_name"] = "実績不足"
        explanation["formula"] = "計算に必要な実績データがありません"
        return explanation
    
    df_recs = records_df.copy()
    df_recs["date"] = pd.to_datetime(df_recs["年月"].astype(str).str.replace("年", "-").str.replace("月", "-01")).dt.date
    
    # 前年度の期間を定義 (4月始まり)
    # target_dateが属する年度の「前年度」
    current_fiscal_year = target_date.year if target_date.month >= 4 else target_date.year - 1
    last_fiscal_year = current_fiscal_year - 1
    prev_fy_start = datetime.date(last_fiscal_year, 4, 1)
    prev_fy_end = datetime.date(last_fiscal_year + 1, 3, 31)
    
    # 前年度実績がフルにあるか判定（開所日が前年度の4/1以前か）
    has_full_prev_fy = opening_date <= prev_fy_start
    
    # --- ケース4: 前年度実績がある場合 (年度更新後) ---
    if has_full_prev_fy:
        target_start = prev_fy_start
        target_end = prev_fy_end
        rule_name = f"【前年度実績】({prev_fy_start.strftime('%Y年%m月')} ～ {prev_fy_end.strftime('%Y年%m月')})"
        
    # --- ケース2: 7ヶ月目～12ヶ月目 (months_passed: 6~11) ---
    elif months_passed < 12:
        # 直近6ヶ月
        target_end = target_ym - datetime.timedelta(days=1) # 前月末
        target_start = target_end.replace(day=1) - relativedelta(months=5) # 6ヶ月前
        rule_name = f"【直近6ヶ月実績】({target_start.strftime('%Y年%m月')} ～ {target_end.strftime('%Y年%m月')})"
        
    # --- ケース3: 13ヶ月目以降だが前年度実績なし (months_passed: 12~) ---
    else:
        # 直近12ヶ月
        target_end = target_ym - datetime.timedelta(days=1) # 前月末
        target_start = target_end.replace(day=1) - relativedelta(months=11) # 12ヶ月前
        rule_name = f"【直近12ヶ月実績】({target_start.strftime('%Y年%m月')} ～ {target_end.strftime('%Y年%m月')})"

    # 対象期間のデータを抽出
    mask = (df_recs["date"] >= target_start) & (df_recs["date"] <= target_end)
    target_df = df_recs[mask].sort_values("date")
    
    if target_df.empty:
        explanation["rule_name"] = "実績不足"
        explanation["formula"] = f"{rule_name} の期間の実績が入力されていません"
        return explanation
    
    total_users = target_df["延べ利用者数"].sum()
    total_days = target_df["開所日数"].sum()
    
    if total_days == 0:
        result = 0.0
    else:
        raw_avg = total_users / total_days
        result = ceil_decimal_1(raw_avg)
    
    explanation["rule_name"] = rule_name
    explanation["details_df"] = target_df[["年月", "延べ利用者数", "開所日数"]]
    explanation["formula"] = f"延べ {total_users}人 ÷ 開所 {total_days}日 ＝ 平均利用人数 {result}人 (計算値: {raw_avg:.3f})"
    explanation["result"] = result
    
    return explanation

def load_data():
    data = {}
    data["settings"] = load_settings_from_sheet()

    default_staff = pd.DataFrame([
        {"名前": "管理者A", "職種(主)": "管理者", "職種(副)": "なし", "雇用形態": "常勤", "契約時間(週)": 40.0, "兼務時間(週)": 0.0, "基本シフト": "A", "固定休": "土,日", "入社日": "2024-04-01", "退職日": ""},
    ])
    df_staff = load_data_from_sheet("staff_master", default_staff)
    
    required_cols_staff = ["名前", "職種(主)", "職種(副)", "雇用形態", "契約時間(週)", "兼務時間(週)", "基本シフト", "固定休", "入社日", "退職日"]
    for col in required_cols_staff:
        if col not in df_staff.columns: df_staff[col] = None

    df_staff["入社日"] = df_staff["入社日"].apply(safe_to_date)
    df_staff["退職日"] = df_staff["退職日"].apply(safe_to_date)
    df_staff["契約時間(週)"] = pd.to_numeric(df_staff["契約時間(週)"], errors='coerce').fillna(0.0)
    df_staff["兼務時間(週)"] = pd.to_numeric(df_staff["兼務時間(週)"], errors='coerce').fillna(0.0)
    data["staff"] = df_staff

    # 利用者マスタ
    default_users = pd.DataFrame([
        {"利用者名": "山田太郎", "利用開始日": "2025-01-01", "利用終了日": "", "支給決定量タイプ": "原則日数(月-8)", "固定日数": 0}
    ])
    df_users = load_data_from_sheet("users_master", default_users)
    required_cols_users = ["利用者名", "利用開始日", "利用終了日", "支給決定量タイプ", "固定日数"]
    for col in required_cols_users:
        if col not in df_users.columns: df_users[col] = None
    df_users["利用開始日"] = df_users["利用開始日"].apply(safe_to_date)
    df_users["利用終了日"] = df_users["利用終了日"].apply(safe_to_date)
    df_users["固定日数"] = pd.to_numeric(df_users["固定日数"], errors='coerce').fillna(0)
    data["users"] = df_users

    default_patterns = pd.DataFrame([
        {"コード": "A", "名称": "日勤A", "開始": "09:00:00", "終了": "16:00:00", "休憩(分)": 60},
    ])
    df_ptn = load_data_from_sheet("shift_patterns", default_patterns)
    df_ptn["開始"] = pd.to_datetime(df_ptn["開始"], format='%H:%M:%S').dt.time
    df_ptn["終了"] = pd.to_datetime(df_ptn["終了"], format='%H:%M:%S').dt.time
    data["patterns"] = df_ptn

    default_holidays = pd.DataFrame([
        {"名称": "年末年始", "開始月": 12, "開始日": 29, "終了月": 1, "終了日": 3},
    ])
    data["holidays"] = load_data_from_sheet("holidays", default_holidays)

    default_records = pd.DataFrame(columns=["年月", "延べ利用者数", "開所日数"])
    data["records"] = load_data_from_sheet("monthly_records", default_records)

    data["draft_shift"] = load_data_from_sheet("current_shift_draft", pd.DataFrame())
    if data["draft_shift"].empty:
        data["draft_shift"] = None 

    return data

def reload_all_data():
    if 'data_loaded' in st.session_state:
        del st.session_state['data_loaded']
    st.rerun()

# ==========================================
# 4. アプリケーション開始
# ==========================================

st.set_page_config(page_title="就労B型 管理システム (Cloud版)", layout="wide")

if 'data_loaded' not in st.session_state:
    with st.spinner("スプレッドシートからデータを読み込んでいます..."):
        data = load_data()
        st.session_state.settings = data["settings"]
        st.session_state.staff_db = data["staff"]
        st.session_state.users_db = data["users"]
        st.session_state.shift_patterns = data["patterns"]
        st.session_state.special_holidays_list = data["holidays"]
        st.session_state.monthly_records = data["records"]
        st.session_state.current_shift_df = data["draft_shift"]
        if "temp_users_calc" not in st.session_state:
            st.session_state.temp_users_calc = None
        st.session_state.data_loaded = True

today = datetime.date.today()
year_range = list(range(today.year - 2, today.year + 3))

st.sidebar.title("メニュー")
menu = st.sidebar.radio(
    "表示する画面を選択",
    ["マスタ・休暇設定", "従業員マスタ", "利用者マスタ", "実績・人員計算", "シフト作成"],
    index=3
)
st.sidebar.divider()
st.sidebar.header("⚙️ 事業所全体設定")
with st.sidebar.expander("詳細設定を開く"):
    with st.form("settings_form"):
        st.subheader("基本情報")
        s_fac_name = st.text_input("事業所名", value=st.session_state.settings["facility_name"])
        s_open_date = st.date_input("開所年月日", value=st.session_state.settings["opening_date"])
        
        current_cap = get_capacity_at_date(today, st.session_state.settings.get('capacity_history', []))
        st.info(f"現在の定員: **{current_cap}名** (履歴管理中)")
        
        st.subheader("体制・営業時間")
        s_ratio_val = st.selectbox("配置基準", [6.0, 7.5, 10.0], index=[6.0, 7.5, 10.0].index(st.session_state.settings.get("service_ratio", 6.0)), format_func=lambda x: RATIO_MAP.get(x, f"{x}:1"))
        s_open_time = st.time_input("営業開始", value=st.session_state.settings["open_time"])
        s_close_time = st.time_input("営業終了", value=st.session_state.settings["close_time"])
        s_fulltime = st.number_input("常勤時間(週)", value=st.session_state.settings["fulltime_hours"], step=0.5)
        
        st.subheader("定休日設定")
        s_closed_days = st.multiselect("曜日定休", ["月", "火", "水", "木", "金", "土", "日"], default=st.session_state.settings["closed_days"])
        s_close_holiday = st.checkbox("祝日は休みにする", value=st.session_state.settings["close_on_holiday"])
        
        if st.form_submit_button("設定を保存"):
            new_settings = st.session_state.settings.copy()
            new_settings.update({
                "facility_name": s_fac_name, "opening_date": s_open_date,
                "open_time": s_open_time, "close_time": s_close_time, "fulltime_hours": s_fulltime,
                "closed_days": s_closed_days, "close_on_holiday": s_close_holiday, "service_ratio": s_ratio_val
            })
            st.session_state.settings = new_settings
            save_settings_to_sheet(new_settings)
            st.success("設定を保存しました")
            reload_all_data()

fulltime_weekly_hours = st.session_state.settings["fulltime_hours"]
service_ratio = st.session_state.settings.get("service_ratio", 6.0)
closed_days_select = st.session_state.settings["closed_days"]
close_on_holiday = st.session_state.settings["close_on_holiday"]

# ==========================================
# メイン画面 (メニュー分岐)
# ==========================================

if menu == "マスタ・休暇設定":
    st.header("🛠️ マスタ・休暇設定")
    st.subheader("1. 勤務区分設定")
    c_p1, c_p2 = st.columns([2, 1])
    with c_p1:
        edited_patterns = st.data_editor(st.session_state.shift_patterns, num_rows="dynamic", use_container_width=True, key="pattern_editor")
    with c_p2:
        if st.button("勤務区分を保存"):
            st.session_state.shift_patterns = edited_patterns
            save_data_to_sheet("shift_patterns", edited_patterns)
            st.success("保存しました"); reload_all_data()
    st.divider()
    st.subheader("2. 定員数の変更履歴")
    col_cap1, col_cap2 = st.columns([2, 1])
    with col_cap1:
        curr_cap_hist = st.session_state.settings.get("capacity_history", [])
        df_cap = pd.DataFrame(curr_cap_hist)
        if "start" not in df_cap.columns: df_cap["start"] = pd.Series(dtype='datetime64[ns]')
        if "count" not in df_cap.columns: df_cap["count"] = 20
        df_cap["start"] = df_cap["start"].apply(safe_to_date)
        df_cap["count"] = pd.to_numeric(df_cap["count"], errors='coerce').fillna(20)
        cap_col_cfg = {"start": st.column_config.DateColumn("開始日", required=True), "count": st.column_config.NumberColumn("定員数", min_value=20, max_value=60, step=1, required=True)}
        new_cap_df = st.data_editor(df_cap, column_config=cap_col_cfg, num_rows="dynamic", use_container_width=True, key="editor_capacity")
    with col_cap2:
        if st.button("定員履歴を保存"):
            def df_to_list_cap(df):
                res = []
                for _, row in df.iterrows():
                    s = row["start"]; c = row["count"]
                    if not s: continue
                    if isinstance(s, pd.Timestamp): s = s.date()
                    res.append({"start": s, "count": int(c)})
                return res
            new_settings = st.session_state.settings.copy()
            new_settings["capacity_history"] = df_to_list_cap(new_cap_df)
            st.session_state.settings = new_settings
            save_settings_to_sheet(new_settings)
            st.success("保存しました"); reload_all_data()
    st.divider()
    st.subheader("3. 加算取得期間の設定")
    col_a1, col_a2, col_a3 = st.columns(3)
    def render_history_editor(key, title):
        current_list = st.session_state.settings.get(key, [])
        df_hist = pd.DataFrame(current_list)
        if "start" not in df_hist.columns: df_hist["start"] = pd.Series(dtype='datetime64[ns]')
        if "end" not in df_hist.columns: df_hist["end"] = pd.Series(dtype='datetime64[ns]')
        df_hist["start"] = df_hist["start"].apply(safe_to_date)
        df_hist["end"] = df_hist["end"].apply(safe_to_date)
        column_cfg = {"start": st.column_config.DateColumn("開始日", required=True), "end": st.column_config.DateColumn("終了日")}
        st.markdown(f"**{title}**")
        return st.data_editor(df_hist, column_config=column_cfg, num_rows="dynamic", use_container_width=True, key=f"editor_{key}")
    with col_a1: new_wage_df = render_history_editor("wage_history", "目標工賃達成指導員加算")
    with col_a2: new_trans_df = render_history_editor("transport_history", "送迎加算")
    with col_a3: new_lunch_df = render_history_editor("lunch_history", "食事提供加算")
    if st.button("加算設定を保存"):
        def df_to_list(df):
            res = []
            for _, row in df.iterrows():
                s, e = row["start"], row["end"]
                if not s: continue 
                if isinstance(s, pd.Timestamp): s = s.date()
                if isinstance(e, pd.Timestamp): e = e.date()
                if pd.isna(s): continue
                if pd.isna(e): e = None
                res.append({"start": s, "end": e})
            return res
        new_settings = st.session_state.settings.copy()
        new_settings["wage_history"] = df_to_list(new_wage_df)
        new_settings["transport_history"] = df_to_list(new_trans_df)
        new_settings["lunch_history"] = df_to_list(new_lunch_df)
        st.session_state.settings = new_settings
        save_settings_to_sheet(new_settings)
        st.success("保存しました"); reload_all_data()
    st.divider()
    st.subheader("4. 毎年繰り返す特別休暇")
    c_h1, c_h2 = st.columns([2, 1])
    with c_h1:
        column_config_holiday = {"名称": st.column_config.TextColumn("休暇名", required=True), "開始月": st.column_config.NumberColumn("開始月", min_value=1, max_value=12), "開始日": st.column_config.NumberColumn("開始日", min_value=1, max_value=31), "終了月": st.column_config.NumberColumn("終了月", min_value=1, max_value=12), "終了日": st.column_config.NumberColumn("終了日", min_value=1, max_value=31)}
        edited_holidays = st.data_editor(st.session_state.special_holidays_list, column_config=column_config_holiday, num_rows="dynamic", use_container_width=True, key="holiday_editor_rec")
    with c_h2:
        if st.button("特別休暇を保存"):
            st.session_state.special_holidays_list = edited_holidays
            save_data_to_sheet("holidays", edited_holidays)
            st.success("保存しました"); reload_all_data()

elif menu == "従業員マスタ":
    st.header("👥 従業員マスタ")
    st.info("※「兼務時間」に入力した時間は、主たる職種の時間から差し引かれ、従たる職種の時間として計算されます。")
    active_staff_df = get_active_staff_df(st.session_state.staff_db, st.session_state.settings, target_date_obj=None)
    shift_codes = st.session_state.shift_patterns["コード"].tolist() if not st.session_state.shift_patterns.empty else []
    job_options = ["管理者", "サービス管理責任者", "職業指導員", "生活支援員", "目標工賃達成指導員", "調理員", "運転手", "事務員", "看護職員", "なし"]
    staff_col_config = {
        "職種(主)": st.column_config.SelectboxColumn("職種(主)", options=job_options, required=True),
        "職種(副)": st.column_config.SelectboxColumn("職種(副)", options=job_options, required=False),
        "雇用形態": st.column_config.SelectboxColumn("雇用形態", options=["常勤", "非常勤"], required=True),
        "基本シフト": st.column_config.SelectboxColumn("基本シフト", options=shift_codes, required=True),
        "契約時間(週)": st.column_config.NumberColumn("契約時間(週)", format="%.1f h", step=0.5),
        "兼務時間(週)": st.column_config.NumberColumn("兼務時間(週)", format="%.1f h", step=0.5, help="職種(副)に従事する時間"),
        "入社日": st.column_config.DateColumn("入社日", required=True),
        "退職日": st.column_config.DateColumn("退職日"),
    }
    edited_staff_df = st.data_editor(active_staff_df, column_config=staff_col_config, num_rows="dynamic", use_container_width=True, key="staff_editor")
    if st.button("従業員情報を保存", type="primary"):
        final_df = edited_staff_df.copy()
        for idx, row in final_df.iterrows():
            if row["雇用形態"] == "常勤": final_df.at[idx, "契約時間(週)"] = fulltime_weekly_hours
        st.session_state.staff_db = final_df 
        save_data_to_sheet("staff_master", final_df) 
        st.success("保存しました"); reload_all_data()

elif menu == "利用者マスタ":
    st.header("🧑‍🤝‍🧑 利用者マスタ")
    df_users = st.session_state.users_db.copy()
    user_col_config = {
        "利用者名": st.column_config.TextColumn("氏名", required=True),
        "利用開始日": st.column_config.DateColumn("利用開始日", required=True),
        "利用終了日": st.column_config.DateColumn("利用終了日"),
        "支給決定量タイプ": st.column_config.SelectboxColumn("支給決定量", options=["原則日数(月-8)", "固定日数"], required=True),
        "固定日数": st.column_config.NumberColumn("固定日数(日)", min_value=0, max_value=31, step=1)
    }
    edited_users_df = st.data_editor(df_users, column_config=user_col_config, num_rows="dynamic", use_container_width=True, key="users_editor")
    if st.button("利用者情報を保存", type="primary"):
        st.session_state.users_db = edited_users_df
        save_data_to_sheet("users_master", edited_users_df)
        st.success("保存しました"); reload_all_data()

elif menu == "実績・人員計算":
    st.header("📊 実績入力と必要人員計算")
    st.subheader("1. 月次実績の入力")
    col_in1, col_in2 = st.columns([1, 2])
    with col_in1:
        s_year_rec = st.selectbox("対象年", year_range, index=year_range.index(today.year))
        s_month_rec = st.selectbox("対象月", list(range(1, 13)), index=today.month - 1)
        target_ym = f"{s_year_rec}年{s_month_rec}月"
        st.caption(f"登録データ名: **{target_ym}**")
        
        st.markdown("---")
        st.write("🧑‍🤝‍🧑 **利用者ごとの実績入力**")
        
        if st.button("マスタから初期値をロード"):
            calc_start = datetime.date(s_year_rec, s_month_rec, 1)
            calc_last_day = calendar.monthrange(s_year_rec, s_month_rec)[1]
            calc_end = datetime.date(s_year_rec, s_month_rec, calc_last_day)
            principle_days = calc_last_day - 8
            
            users_df = st.session_state.users_db
            temp_rows = []
            
            for _, u in users_df.iterrows():
                start = u["利用開始日"]; end = u["利用終了日"]
                if not start: continue
                if start > calc_end: continue
                if end and end < calc_start: continue
                
                u_days = 0
                if u["支給決定量タイプ"] == "原則日数(月-8)": u_days = principle_days
                else: u_days = int(u["固定日数"]) if pd.notnull(u["固定日数"]) else 0
                
                temp_rows.append({"利用者名": u["利用者名"], "予定日数": u_days, "実績日数": u_days, "備考": ""})
            
            st.session_state.temp_users_calc = pd.DataFrame(temp_rows)
        
        calculated_total = 0
        if st.session_state.temp_users_calc is not None:
            user_calc_config = {
                "利用者名": st.column_config.TextColumn(disabled=True),
                "予定日数": st.column_config.NumberColumn(disabled=True),
                "実績日数": st.column_config.NumberColumn(min_value=0, max_value=31, step=1, required=True),
                "備考": st.column_config.TextColumn()
            }
            edited_calc_df = st.data_editor(
                st.session_state.temp_users_calc,
                column_config=user_calc_config,
                use_container_width=True,
                num_rows="fixed",
                hide_index=True,
                key="calc_editor"
            )
            calculated_total = edited_calc_df["実績日数"].sum()
            st.metric("合計延べ利用者数 (集計結果)", f"{calculated_total} 人")
        else:
            st.info("上の「ロード」ボタンを押してリストを表示してください")

        start_date = datetime.date(s_year_rec, s_month_rec, 1)
        last_day = calendar.monthrange(s_year_rec, s_month_rec)[1]
        temp_open_days = 0
        for d_int in range(1, last_day + 1):
            curr = datetime.date(s_year_rec, s_month_rec, d_int)
            wd = JP_DAYS[curr.weekday()]
            if wd not in closed_days_select and not (close_on_holiday and jpholiday.is_holiday(curr)):
                if not is_special_holiday_recurring(curr, st.session_state.special_holidays_list)[0]:
                    temp_open_days += 1
        
        current_cap = get_capacity_at_date(start_date, st.session_state.settings.get('capacity_history', []))
        
        if temp_open_days > 0 and calculated_total > 0:
            daily_avg = calculated_total / temp_open_days
            if daily_avg > current_cap * 1.2:
                st.error(f"⚠️ 月平均利用率 {daily_avg/current_cap:.0%}。特定日の定員超過リスクがあります。")

    with col_in2:
        st.metric("自動計算された開所日数", f"{temp_open_days} 日")
        
        if st.button("集計結果を実績として保存", type="primary", disabled=(calculated_total==0)):
            df_recs = st.session_state.monthly_records
            df_recs = df_recs[df_recs["年月"] != target_ym]
            new_row = {"年月": target_ym, "延べ利用者数": calculated_total, "開所日数": temp_open_days}
            st.session_state.monthly_records = pd.concat([df_recs, pd.DataFrame([new_row])], ignore_index=True)
            save_data_to_sheet("monthly_records", st.session_state.monthly_records)
            st.success(f"{target_ym} の実績（{calculated_total}人）を保存しました")
            if "temp_users_input" in st.session_state:
                del st.session_state["temp_users_input"]
            reload_all_data()

    st.divider()

    st.subheader("2. 平均利用人数と人員配置チェック")
    col_cy, col_cm = st.columns(2)
    with col_cy: c_year_calc = st.selectbox("計算対象年", year_range, index=year_range.index(today.year), key="calc_y")
    with col_cm: c_month_calc = st.selectbox("計算対象月", list(range(1, 13)), index=today.month - 1, key="calc_m")
    calc_target_date = datetime.date(c_year_calc, c_month_calc, 1)
    
    warning_messages = []
    sets = st.session_state.settings
    def check_addon_period_strict(history_key, roles, name):
        is_active = is_addon_active(calc_target_date, sets.get(history_key, []))
        if is_active:
            valid_staff = get_active_staff_df(st.session_state.staff_db, sets, target_date_obj=calc_target_date)
            has_role = False
            for _, r in valid_staff.iterrows():
                if r["職種(主)"] in roles or r["職種(副)"] in roles:
                    has_role = True
                    break
            if not has_role: warning_messages.append(f"⚠️ {name}期間中ですが有効な『{'・'.join(roles)}』が不在です。")
        return is_active
    wage_active = check_addon_period_strict("wage_history", ["目標工賃達成指導員"], "目標工賃達成指導員加算")
    check_addon_period_strict("transport_history", ["運転手"], "送迎加算")
    check_addon_period_strict("lunch_history", ["調理員"], "食事提供加算")
    if warning_messages:
        for msg in warning_messages: st.error(msg)
    else: st.success("✅ 加算要件OK")

    calc_result = calculate_average_users_detail(calc_target_date, st.session_state.settings["opening_date"], st.session_state.settings.get("capacity_history", []), st.session_state.monthly_records)
    avg_users = calc_result["result"]
    
    c_res1, c_res2 = st.columns([1.5, 1])
    with c_res1:
        st.info(f"適用ルール: **{calc_result['rule_name']}**")
        st.metric("確定: 平均利用人数", f"{avg_users} 人")
        if calc_result["details_df"] is not None and not calc_result["details_df"].empty:
            with st.expander("計算根拠"): st.dataframe(calc_result["details_df"], use_container_width=True)
        st.markdown(f"**計算式:** {calc_result['formula']}")

    with c_res2:
        base_staff_raw = avg_users / service_ratio
        base_staff_req = ceil_decimal_1(base_staff_raw) # 基礎分（切り上げ）
        wage_staff_req = 1.0 if wage_active else 0.0 # 加算分
        total_req = base_staff_req + wage_staff_req
        
        display_ratio = RATIO_MAP.get(service_ratio, f"{service_ratio}:1")
        st.markdown(f"**必要人員合計 ({display_ratio}): {total_req} 人**")
        
        # 内訳表示
        st.write(f"- 平均利用人数 {avg_users}人 ÷ {service_ratio} ＝ {base_staff_raw:.3f}...")
        st.write(f"　→ 切り上げ **{base_staff_req}人** (基礎配置)")
        if wage_staff_req > 0:
            st.write(f"- ＋ 目標工賃達成指導員 **{wage_staff_req}人**")
        
        st.markdown("**現在のマスタと照合（兼務考慮）**")
        current_staff_df = get_active_staff_df(st.session_state.staff_db, st.session_state.settings, target_date_obj=calc_target_date)
        actual_base_fte = 0.0
        actual_wage_fte = 0.0
        base_roles = ["職業指導員", "生活支援員"]
        wage_roles = ["目標工賃達成指導員"]
        details_log = []
        
        for _, staff in current_staff_df.iterrows():
            total_h = staff["契約時間(週)"]; sub_h = staff["兼務時間(週)"]
            try: total_h=float(total_h); sub_h=float(sub_h)
            except: total_h=0; sub_h=0
            main_h = max(0, total_h - sub_h)
            
            if staff["職種(主)"] in base_roles:
                fte = main_h / fulltime_weekly_hours; actual_base_fte += fte; 
                if fte>0: details_log.append(f"{staff['名前']}(主): 支援員等 {fte:.2f}人分")
            elif staff["職種(主)"] in wage_roles:
                fte = main_h / fulltime_weekly_hours; actual_wage_fte += fte; 
                if fte>0: details_log.append(f"{staff['名前']}(主): 目標工賃 {fte:.2f}人分")
            
            if staff["職種(副)"] in base_roles:
                fte = sub_h / fulltime_weekly_hours; actual_base_fte += fte; 
                if fte>0: details_log.append(f"{staff['名前']}(副): 支援員等 {fte:.2f}人分")
            elif staff["職種(副)"] in wage_roles:
                fte = sub_h / fulltime_weekly_hours; actual_wage_fte += fte; 
                if fte>0: details_log.append(f"{staff['名前']}(副): 目標工賃 {fte:.2f}人分")

        total_actual = actual_base_fte + actual_wage_fte
        st.metric("配置可能人員", f"{total_actual:.2f} 人")
        
        is_base_ok = actual_base_fte >= base_staff_req
        is_wage_ok = actual_wage_fte >= wage_staff_req
        
        if is_base_ok and is_wage_ok: st.success("✅ 充足")
        else:
            st.error("❌ 不足あり")
            if not is_base_ok: st.write(f"- 支援員等が {round(base_staff_req - actual_base_fte, 2)}人 不足")
            if not is_wage_ok: st.write(f"- 目標工賃担当が {round(wage_staff_req - actual_wage_fte, 2)}人 不足")
        
        with st.expander("詳細内訳"):
            for d in details_log: st.write(f"- {d}")

elif menu == "シフト作成":
    st.header("📝 シフト作成")
    col_sy, col_sm = st.columns(2)
    with col_sy: s_year_shift = st.selectbox("作成年", year_range, index=year_range.index(today.year), key="shift_y")
    with col_sm: s_month_shift = st.selectbox("作成月", list(range(1, 13)), index=today.month - 1, key="shift_m")
    shift_month = datetime.date(s_year_shift, s_month_shift, 1)
    
    shift_staff_df = get_active_staff_df(st.session_state.staff_db, st.session_state.settings, target_date_obj=shift_month)
    shift_staff_names = shift_staff_df["名前"].tolist()
    shift_opts = st.session_state.shift_patterns["コード"].tolist() + ["休", "公休", "有給"]
    
    start_dt = shift_month.replace(day=1)
    end_dt = start_dt + relativedelta(months=1) - datetime.timedelta(days=1)
    dates = pd.date_range(start_dt, end_dt)
    date_cols = []; holiday_cols = []
    
    for d in dates:
        d_label = f"{d.day}({JP_DAYS[d.weekday()]})"
        date_cols.append(d_label)
        is_holiday = False
        wd_str = JP_DAYS[d.weekday()]
        if wd_str in closed_days_select: is_holiday = True
        elif close_on_holiday and jpholiday.is_holiday(d.date()): is_holiday = True
        else:
            is_sp, _ = is_special_holiday_recurring(d.date(), st.session_state.special_holidays_list)
            if is_sp: is_holiday = True
        if is_holiday: holiday_cols.append(d_label)

    if st.button("シフト案を新規自動生成", type="primary"):
        rows = []
        for _, staff in shift_staff_df.iterrows():
            s_name = staff["名前"]
            row_data = {"氏名": s_name}
            for d in dates:
                d_label = f"{d.day}({JP_DAYS[d.weekday()]})"
                is_closed = (d_label in holiday_cols)
                if is_closed: row_data[d_label] = "休"
                elif JP_DAYS[d.weekday()] in staff["固定休"]: row_data[d_label] = "公休"
                else: row_data[d_label] = staff["基本シフト"]
            rows.append(row_data)
        new_df = pd.DataFrame(rows)
        st.session_state.current_shift_df = new_df
        save_data_to_sheet("current_shift_draft", new_df)
        st.success("新規作成しました"); reload_all_data()

    if st.session_state.current_shift_df is not None:
        current_df = st.session_state.current_shift_df
        column_config = {"氏名": st.column_config.TextColumn("氏名", disabled=True)}
        for d_col in date_cols:
            if d_col in current_df.columns:
                column_config[d_col] = st.column_config.SelectboxColumn(d_col, options=shift_opts, required=True, width="small")
        display_cols = ["氏名"] + [c for c in date_cols if c in current_df.columns]
        edited_df = st.data_editor(current_df[display_cols], column_config=column_config, use_container_width=True, height=400, hide_index=True, key="shift_editor_h_key")
        st.session_state.current_shift_df = edited_df
        save_data_to_sheet("current_shift_draft", edited_df)
        
        def highlight_holidays_col(data):
            style_df = pd.DataFrame('', index=data.index, columns=data.columns)
            for col in holiday_cols:
                if col in style_df.columns: style_df[col] = 'background-color: #ffe6e6; color: #cc0000'
            return style_df
        st.dataframe(edited_df.style.apply(highlight_holidays_col, axis=None), use_container_width=True, height=600, hide_index=True)
        csv_out = edited_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("シフト表をPCに保存 (CSV)", csv_out, "shift_h_final.csv", "text/csv")
