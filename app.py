import streamlit as st
import pandas as pd
import jpholiday
import math
import datetime
import calendar
import json
from dateutil.relativedelta import relativedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==========================================
# 1. Google Sheets 接続設定
# ==========================================

@st.cache_resource
def get_gspread_client():
    key_dict = st.secrets["gcp_service_account"]
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    client = gspread.authorize(creds)
    return client

def get_spreadsheet():
    client = get_gspread_client()
    sheet_url = st.secrets["spreadsheet"]["url"]
    return client.open_by_url(sheet_url)

# --- データの読み書き関数 ---

def load_data_from_sheet(worksheet_name, default_df=None):
    sh = get_spreadsheet()
    try:
        worksheet = sh.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        if not data and default_df is not None:
            return default_df
        return pd.DataFrame(data)
    except gspread.WorksheetNotFound:
        if default_df is not None:
            worksheet = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
            save_data_to_sheet(worksheet_name, default_df)
            return default_df
        return pd.DataFrame()

def save_data_to_sheet(worksheet_name, df):
    sh = get_spreadsheet()
    try:
        worksheet = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
    
    worksheet.clear()
    params = [df.columns.values.tolist()] + df.values.tolist()
    clean_params = []
    for row in params:
        clean_row = []
        for cell in row:
            if isinstance(cell, (datetime.date, datetime.datetime, datetime.time)):
                clean_row.append(str(cell))
            elif pd.isna(cell):
                clean_row.append("")
            else:
                clean_row.append(cell)
        clean_params.append(clean_row)
        
    worksheet.update(clean_params)

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
    json_str = json.dumps(s_save, ensure_ascii=False)
    
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet("settings")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="settings", rows=10, cols=10)
    ws.update_acell('A1', json_str)

# ==========================================
# 2. アプリ共通設定
# ==========================================

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
    "wage_flag": True, "wage_start": "2024-11-01",
    "transport_flag": True, "transport_start": "2024-11-01",
    "lunch_flag": False, "lunch_start": "2024-11-01",
    "add_ons": [] 
}

RATIO_MAP = {6.0: "6:1", 7.5: "7.5:1", 10.0: "10:1"}

def _get_default_settings_obj():
    s = DEFAULT_SETTINGS.copy()
    for k in ["opening_date", "wage_start", "transport_start", "lunch_start"]:
        if isinstance(s[k], str): s[k] = datetime.datetime.strptime(s[k], "%Y-%m-%d").date()
    for k in ["open_time", "close_time"]:
        if isinstance(s[k], str): s[k] = datetime.datetime.strptime(s[k], "%H:%M:%S").time()
    return s

def ceil_decimal_1(value):
    return math.ceil(value * 10) / 10

def load_data():
    data = {}
    data["settings"] = load_settings_from_sheet()

    # スタッフマスタ（兼務時間を追加）
    default_staff = pd.DataFrame([
        {"名前": "管理者A", "職種(主)": "管理者", "職種(副)": "なし", "雇用形態": "常勤", "契約時間(週)": 40.0, "兼務時間(週)": 0.0, "基本シフト": "A", "固定休": "土,日", "入社日": "2024-04-01", "退職日": ""},
        {"名前": "サビ管B", "職種(主)": "サービス管理責任者", "職種(副)": "なし", "雇用形態": "常勤", "契約時間(週)": 40.0, "兼務時間(週)": 0.0, "基本シフト": "A", "固定休": "土,日", "入社日": "2024-04-01", "退職日": ""},
        {"名前": "指導員C", "職種(主)": "職業指導員", "職種(副)": "運転手", "雇用形態": "常勤", "契約時間(週)": 40.0, "兼務時間(週)": 5.0, "基本シフト": "A", "固定休": "日,月", "入社日": "2024-04-01", "退職日": ""},
        {"名前": "支援員D", "職種(主)": "生活支援員", "職種(副)": "調理員", "雇用形態": "非常勤", "契約時間(週)": 20.0, "兼務時間(週)": 0.0, "基本シフト": "午", "固定休": "火,木,土,日", "入社日": "2024-04-01", "退職日": ""},
    ])
    df_staff = load_data_from_sheet("staff_master", default_staff)
    df_staff["入社日"] = pd.to_datetime(df_staff["入社日"]).dt.date
    df_staff["退職日"] = pd.to_datetime(df_staff["退職日"], errors='coerce').dt.date
    df_staff["契約時間(週)"] = pd.to_numeric(df_staff["契約時間(週)"], errors='coerce').fillna(0.0)
    # 兼務時間がなければ追加
    if "兼務時間(週)" not in df_staff.columns:
        df_staff["兼務時間(週)"] = 0.0
    df_staff["兼務時間(週)"] = pd.to_numeric(df_staff["兼務時間(週)"], errors='coerce').fillna(0.0)
    
    data["staff"] = df_staff

    default_patterns = pd.DataFrame([
        {"コード": "A", "名称": "日勤A", "開始": "09:00:00", "終了": "16:00:00", "休憩(分)": 60},
        {"コード": "B", "名称": "日勤B", "開始": "09:00:00", "終了": "17:00:00", "休憩(分)": 60},
        {"コード": "早", "名称": "早番",  "開始": "08:30:00", "終了": "16:30:00", "休憩(分)": 60},
        {"コード": "午", "名称": "午前",  "開始": "09:00:00", "終了": "13:00:00", "休憩(分)": 0},
    ])
    df_ptn = load_data_from_sheet("shift_patterns", default_patterns)
    df_ptn["開始"] = pd.to_datetime(df_ptn["開始"], format='%H:%M:%S').dt.time
    df_ptn["終了"] = pd.to_datetime(df_ptn["終了"], format='%H:%M:%S').dt.time
    data["patterns"] = df_ptn

    default_holidays = pd.DataFrame([
        {"名称": "年末年始", "開始月": 12, "開始日": 29, "終了月": 1, "終了日": 3},
        {"名称": "夏季休暇", "開始月": 8,  "開始日": 13, "終了月": 8, "終了日": 15},
    ])
    data["holidays"] = load_data_from_sheet("holidays", default_holidays)

    default_records = pd.DataFrame(columns=["年月", "延べ利用者数", "開所日数"])
    data["records"] = load_data_from_sheet("monthly_records", default_records)

    data["draft_shift"] = load_data_from_sheet("current_shift_draft", pd.DataFrame())
    if data["draft_shift"].empty:
        data["draft_shift"] = None 

    return data

# ==========================================
# 3. アプリケーション本体
# ==========================================

st.set_page_config(page_title="就労B型 管理システム (Cloud版)", layout="wide")

if 'data_loaded' not in st.session_state:
    with st.spinner("スプレッドシートからデータを読み込んでいます..."):
        data = load_data()
        st.session_state.settings = data["settings"]
        st.session_state.staff_db = data["staff"]
        st.session_state.shift_patterns = data["patterns"]
        st.session_state.special_holidays_list = data["holidays"]
        st.session_state.monthly_records = data["records"]
        st.session_state.current_shift_df = data["draft_shift"]
        st.session_state.data_loaded = True

# --- ヘルパー関数 ---
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
    if target_date_obj:
        last_day = calendar.monthrange(target_date_obj.year, target_date_obj.month)[1]
        month_end = datetime.date(target_date_obj.year, target_date_obj.month, last_day)
        df["入社日"] = pd.to_datetime(df["入社日"]).dt.date
        df["退職日"] = pd.to_datetime(df["退職日"]).dt.date
        
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

    # ここでは職種フィルタは行わない（マスタでは全員見たい、シフト作成時は計算ロジックで制御）
    # ただしシフト作成画面での表示のために一応返す
    return df

def calculate_average_users_detail(target_date, opening_date, capacity, records_df):
    diff = relativedelta(target_date, opening_date)
    elapsed_months = diff.years * 12 + diff.months 
    explanation = { "rule_name": "", "period_start": "", "period_end": "", "details_df": None, "formula": "", "result": 0.0 }
    
    if elapsed_months < 6:
        explanation["rule_name"] = "【新規開所特例】開所6ヶ月間"
        explanation["formula"] = f"定員 {capacity}人 × 90%"
        explanation["result"] = ceil_decimal_1(capacity * 0.9)
        return explanation

    current_fiscal_year = target_date.year if target_date.month >= 4 else target_date.year - 1
    last_fiscal_year = current_fiscal_year - 1
    prev_start = datetime.date(last_fiscal_year, 4, 1)
    prev_end = datetime.date(last_fiscal_year + 1, 3, 31)
    
    if records_df.empty:
        explanation["rule_name"] = "実績データなし"
        explanation["formula"] = "実績を入力してください"
        return explanation

    df_recs = records_df.copy()
    df_recs["date"] = pd.to_datetime(df_recs["年月"].astype(str).str.replace("年", "-").str.replace("月", "-01"))
    df_recs["dt_date"] = df_recs["date"].dt.date
    
    mask_prev = (df_recs["dt_date"] >= prev_start) & (df_recs["dt_date"] <= prev_end)
    df_prev = df_recs[mask_prev]
    is_experienced = opening_date <= prev_start
    
    target_df = pd.DataFrame()
    rule_text = ""
    
    if is_experienced and not df_prev.empty:
        target_df = df_prev
        rule_text = f"【前年度実績】({prev_start.strftime('%Y年%m月')} ～ {prev_end.strftime('%Y年%m月')})"
    else:
        end_search = target_date.replace(day=1) - datetime.timedelta(days=1)
        start_search_12 = end_search - relativedelta(months=11)
        actual_start = start_search_12
        if opening_date > actual_start: actual_start = opening_date.replace(day=1)
        mask_recent = (df_recs["dt_date"] >= actual_start) & (df_recs["dt_date"] <= end_search)
        df_recent = df_recs[mask_recent].sort_values("dt_date")
        target_df = df_recent
        rule_text = f"【直近実績】({actual_start.strftime('%Y年%m月')} ～ {end_search.strftime('%Y年%m月')})"

    if target_df.empty:
        explanation["rule_name"] = "実績不足"
        explanation["formula"] = "計算に必要な期間のデータがありません"
        return explanation
        
    total_users = target_df["延べ利用者数"].sum()
    total_days = target_df["開所日数"].sum()
    if total_days == 0: result = 0.0
    else:
        raw_avg = total_users / total_days
        result = ceil_decimal_1(raw_avg)
        
    explanation["rule_name"] = rule_text
    explanation["details_df"] = target_df[["年月", "延べ利用者数", "開所日数"]]
    explanation["formula"] = f"延べ {total_users}人 ÷ 開所 {total_days}日 = {raw_avg:.3f}..."
    explanation["result"] = result
    return explanation

# ==========================================
# 3. UI構築
# ==========================================

st.title("🏢 就労B型 運営管理システム")

today = datetime.date.today()
year_range = list(range(today.year - 2, today.year + 3))

# --- サイドバー ---
st.sidebar.header("⚙️ 事業所全体設定")
st.sidebar.caption("変更後に「設定を保存」を押してください")

with st.sidebar.form("settings_form"):
    st.subheader("基本情報")
    s_fac_name = st.text_input("事業所名", value=st.session_state.settings["facility_name"])
    s_open_date = st.date_input("開所年月日", value=st.session_state.settings["opening_date"])
    s_capacity = st.number_input("定員数", value=st.session_state.settings["capacity"], step=1)
    
    st.subheader("体制・営業時間")
    s_ratio_val = st.selectbox("配置基準", [6.0, 7.5, 10.0], index=[6.0, 7.5, 10.0].index(st.session_state.settings.get("service_ratio", 6.0)), format_func=lambda x: RATIO_MAP.get(x, f"{x}:1"))
    s_open_time = st.time_input("営業開始", value=st.session_state.settings["open_time"])
    s_close_time = st.time_input("営業終了", value=st.session_state.settings["close_time"])
    s_fulltime = st.number_input("常勤時間(週)", value=st.session_state.settings["fulltime_hours"], step=0.5)
    
    st.subheader("取得加算と開始時期")
    s_wage_flag = st.checkbox("目標工賃達成指導員加算", value=st.session_state.settings.get("wage_flag", False))
    s_wage_start = st.date_input("開始年月 (目標工賃)", value=st.session_state.settings.get("wage_start", today)) if s_wage_flag else today
    
    s_trans_flag = st.checkbox("送迎加算", value=st.session_state.settings.get("transport_flag", False))
    s_trans_start = st.date_input("開始年月 (送迎)", value=st.session_state.settings.get("transport_start", today)) if s_trans_flag else today
    
    s_lunch_flag = st.checkbox("食事提供加算", value=st.session_state.settings.get("lunch_flag", False))
    s_lunch_start = st.date_input("開始年月 (食事)", value=st.session_state.settings.get("lunch_start", today)) if s_lunch_flag else today
    
    st.subheader("定休日設定")
    s_closed_days = st.multiselect("曜日定休", ["月", "火", "水", "木", "金", "土", "日"], default=st.session_state.settings["closed_days"])
    s_close_holiday = st.checkbox("祝日は休みにする", value=st.session_state.settings["close_on_holiday"])

    if st.form_submit_button("設定を保存"):
        new_settings = {
            "facility_name": s_fac_name, "opening_date": s_open_date, "capacity": s_capacity,
            "open_time": s_open_time, "close_time": s_close_time, "fulltime_hours": s_fulltime,
            "add_ons": st.session_state.settings["add_ons"], # ダミー
            "closed_days": s_closed_days, "close_on_holiday": s_close_holiday,
            "service_ratio": s_ratio_val,
            "wage_flag": s_wage_flag, "wage_start": s_wage_start,
            "transport_flag": s_trans_flag, "transport_start": s_trans_start,
            "lunch_flag": s_lunch_flag, "lunch_start": s_lunch_start
        }
        st.session_state.settings = new_settings
        save_settings_to_sheet(new_settings)
        st.success("設定をクラウドに保存しました")
        st.rerun()

# 変数展開
fulltime_weekly_hours = st.session_state.settings["fulltime_hours"]
service_ratio = st.session_state.settings.get("service_ratio", 6.0)
closed_days_select = st.session_state.settings["closed_days"]
close_on_holiday = st.session_state.settings["close_on_holiday"]

# --- メインエリア ---
tab1, tab2, tab3, tab4 = st.tabs(["🛠️ マスタ・休暇", "👥 従業員マスタ", "📅 実績・人員計算", "📝 シフト作成"])

# ------------------------------------------
# TAB 1: マスタ・休暇
# ------------------------------------------
with tab1:
    col_m1, col_m2 = st.columns(2)
    with col_m1:
        st.subheader("1. 勤務区分設定")
        edited_patterns = st.data_editor(st.session_state.shift_patterns, num_rows="dynamic", use_container_width=True, key="pattern_editor")
        if st.button("勤務区分を保存"):
            st.session_state.shift_patterns = edited_patterns
            save_data_to_sheet("shift_patterns", edited_patterns)
            st.success("勤務区分をクラウドに保存しました")
            st.rerun()

    with col_m2:
        st.subheader("2. 毎年繰り返す特別休暇")
        column_config_holiday = {
            "名称": st.column_config.TextColumn("休暇名", required=True),
            "開始月": st.column_config.NumberColumn("開始月", min_value=1, max_value=12),
            "開始日": st.column_config.NumberColumn("開始日", min_value=1, max_value=31),
            "終了月": st.column_config.NumberColumn("終了月", min_value=1, max_value=12),
            "終了日": st.column_config.NumberColumn("終了日", min_value=1, max_value=31),
        }
        edited_holidays = st.data_editor(st.session_state.special_holidays_list, column_config=column_config_holiday, num_rows="dynamic", use_container_width=True, key="holiday_editor_rec")
        if st.button("特別休暇を保存"):
            st.session_state.special_holidays_list = edited_holidays
            save_data_to_sheet("holidays", edited_holidays)
            st.success("特別休暇をクラウドに保存しました")
            st.rerun()

# ------------------------------------------
# TAB 2: 従業員マスタ
# ------------------------------------------
with tab2:
    st.header("👥 従業員詳細設定")
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
        st.success("従業員データをクラウドに保存しました")
        st.rerun()

# ------------------------------------------
# TAB 3: 実績・人員計算
# ------------------------------------------
with tab3:
    st.header("📊 実績入力と必要人員計算")
    st.subheader("1. 月次実績の入力")
    col_in1, col_in2 = st.columns([1, 2])
    with col_in1:
        s_year_rec = st.selectbox("対象年", year_range, index=year_range.index(today.year))
        s_month_rec = st.selectbox("対象月", list(range(1, 13)), index=today.month - 1)
        target_ym = f"{s_year_rec}年{s_month_rec}月"
        st.caption(f"登録データ名: **{target_ym}**")
        users_input = st.number_input("延べ利用者数", min_value=0, value=400)
    
    with col_in2:
        start_date = datetime.date(s_year_rec, s_month_rec, 1)
        last_day = calendar.monthrange(s_year_rec, s_month_rec)[1]
        calc_open_days = 0
        jp_days = ["月","火","水","木","金","土","日"]
        for d_int in range(1, last_day + 1):
            curr = datetime.date(s_year_rec, s_month_rec, d_int)
            wd = jp_days[curr.weekday()]
            is_closed = False
            if wd in closed_days_select: is_closed = True
            elif close_on_holiday and jpholiday.is_holiday(curr): is_closed = True
            else:
                is_sp, _ = is_special_holiday_recurring(curr, st.session_state.special_holidays_list)
                if is_sp: is_closed = True
            if not is_closed: calc_open_days += 1
        st.metric("自動計算された開所日数", f"{calc_open_days} 日")
        
        if st.button("実績を保存"):
            df_recs = st.session_state.monthly_records
            df_recs = df_recs[df_recs["年月"] != target_ym]
            new_row = {"年月": target_ym, "延べ利用者数": users_input, "開所日数": calc_open_days}
            st.session_state.monthly_records = pd.concat([df_recs, pd.DataFrame([new_row])], ignore_index=True)
            save_data_to_sheet("monthly_records", st.session_state.monthly_records)
            st.success(f"{target_ym} の実績をクラウドに保存しました")
            st.rerun()

    st.divider()

    st.subheader("2. 平均利用人数と人員配置チェック")
    st.markdown("##### 計算基準月の設定（シフト作成対象月）")
    col_cy, col_cm = st.columns(2)
    with col_cy:
        c_year_calc = st.selectbox("計算対象年", year_range, index=year_range.index(today.year), key="calc_y")
    with col_cm:
        c_month_calc = st.selectbox("計算対象月", list(range(1, 13)), index=today.month - 1, key="calc_m")
        
    calc_target_date = datetime.date(c_year_calc, c_month_calc, 1)
    
    # --- 加算要件チェック (兼務含む) ---
    warning_messages = []
    sets = st.session_state.settings
    
    def check_addon_period(flag, start_date, roles, name):
        if flag and calc_target_date >= start_date.replace(day=1):
            valid_staff = get_active_staff_df(st.session_state.staff_db, sets, target_date_obj=calc_target_date)
            # 在籍していて、かつその役割を持っているか（主または副）
            has_role = False
            for _, r in valid_staff.iterrows():
                if r["職種(主)"] in roles or r["職種(副)"] in roles:
                    has_role = True
                    break
            if not has_role:
                warning_messages.append(f"⚠️ {name}を取得中（{start_date}開始）ですが、{calc_target_date.strftime('%Y年%m月')}時点で有効な『{'・'.join(roles)}』がマスタに存在しません。")

    check_addon_period(sets.get("wage_flag"), sets.get("wage_start"), ["目標工賃達成指導員"], "目標工賃達成指導員加算")
    check_addon_period(sets.get("transport_flag"), sets.get("transport_start"), ["運転手"], "送迎加算")
    check_addon_period(sets.get("lunch_flag"), sets.get("lunch_start"), ["調理員"], "食事提供加算")

    if warning_messages:
        for msg in warning_messages: st.error(msg)
    else:
        st.success("✅ 加算要件に対する職種配置はOKです")

    # 計算実行
    calc_result = calculate_average_users_detail(
        calc_target_date, 
        st.session_state.settings["opening_date"], 
        st.session_state.settings["capacity"],
        st.session_state.monthly_records
    )
    avg_users = calc_result["result"]
    
    c_res1, c_res2 = st.columns([1.5, 1])
    with c_res1:
        st.info(f"適用ルール: **{calc_result['rule_name']}**")
        st.metric("確定: 平均利用人数", f"{avg_users} 人")
        if calc_result["details_df"] is not None and not calc_result["details_df"].empty:
            with st.expander("計算根拠（使用した実績データ）を確認する", expanded=True):
                st.dataframe(calc_result["details_df"], use_container_width=True)
                st.markdown(f"**計算式:** {calc_result['formula']} → 切り上げ **{avg_users}**")
        else:
            if calc_result['formula']: st.write(f"計算式: {calc_result['formula']}")

    with c_res2:
        base_staff = avg_users / service_ratio
        
        # 目標工賃達成指導員加算がある場合は +1.0
        add_staff = 0.0
        if sets.get("wage_flag") and calc_target_date >= sets.get("wage_start").replace(day=1):
            add_staff = 1.0
            
        required_staff = ceil_decimal_1(base_staff + add_staff)
        display_ratio = RATIO_MAP.get(service_ratio, f"{service_ratio}:1")
        st.metric(f"必要人員合計 ({display_ratio})", f"{required_staff} 人", help=f"基準配置 {base_staff:.2f} + 加算配置 {add_staff} (端数切り上げ)")
        
        st.markdown("**現在のマスタと照合（兼務考慮）**")
        current_staff_df = get_active_staff_df(st.session_state.staff_db, st.session_state.settings, target_date_obj=calc_target_date)
        
        actual_fte = 0.0
        
        # 配置基準（職業指導員・生活支援員）としてカウントする対象職種
        # ※目標工賃達成指導員も、加算で+1要求される分、カウント対象に含めるのが一般的（自治体によるが）
        # ここではシンプルに「指導員・支援員」＋「目標工賃担当」をカウント対象とする
        target_roles = ["職業指導員", "生活支援員", "目標工賃達成指導員"]
        
        details = []
        
        for _, staff in current_staff_df.iterrows():
            total_hours = staff["契約時間(週)"]
            sub_hours = staff["兼務時間(週)"]
            
            # 主業務の時間 = 全体 - 兼務
            main_hours = max(0, total_hours - sub_hours)
            
            # 加算用の一時変数
            staff_target_hours = 0.0
            
            # 主職種がターゲットなら加算
            if staff["職種(主)"] in target_roles:
                staff_target_hours += main_hours
                
            # 副職種がターゲットなら加算
            if staff["職種(副)"] in target_roles:
                staff_target_hours += sub_hours
                
            if staff_target_hours > 0:
                fte = staff_target_hours / fulltime_weekly_hours
                if fte > 1.0: fte = 1.0
                actual_fte += fte
                details.append(f"{staff['名前']}: {fte:.2f} (対象 {staff_target_hours}h)")

        actual_fte = round(actual_fte, 1)
        st.metric("配置可能人員", f"{actual_fte} 人")
        
        if actual_fte >= required_staff:
            st.success("✅ 充足")
        else:
            st.error(f"❌ 不足 {round(required_staff - actual_fte, 1)}人")
            
        with st.expander("内訳（兼務考慮済）"):
            for d in details: st.write(f"- {d}")

# ------------------------------------------
# TAB 4: シフト作成
# ------------------------------------------
with tab4:
    st.header("📝 シフト作成")
    
    col_sy, col_sm = st.columns(2)
    with col_sy:
        s_year_shift = st.selectbox("作成年", year_range, index=year_range.index(today.year), key="shift_y")
    with col_sm:
        s_month_shift = st.selectbox("作成月", list(range(1, 13)), index=today.month - 1, key="shift_m")
        
    shift_month = datetime.date(s_year_shift, s_month_shift, 1)
    
    shift_staff_df = get_active_staff_df(st.session_state.staff_db, st.session_state.settings, target_date_obj=shift_month)
    shift_staff_names = shift_staff_df["名前"].tolist()
    
    shift_opts = st.session_state.shift_patterns["コード"].tolist() + ["休", "公休", "有給"]
    
    start_dt = shift_month.replace(day=1)
    end_dt = start_dt + relativedelta(months=1) - datetime.timedelta(days=1)
    dates = pd.date_range(start_dt, end_dt)
    
    jp_days = ["月","火","水","木","金","土","日"]
    date_cols = []
    holiday_cols = [] 
    
    for d in dates:
        d_label = f"{d.day}({jp_days[d.weekday()]})"
        date_cols.append(d_label)
        is_holiday = False
        wd_str = jp_days[d.weekday()]
        if wd_str in closed_days_select: is_holiday = True
        elif close_on_holiday and jpholiday.is_holiday(d.date()): is_holiday = True
        else:
            is_sp, _ = is_special_holiday_recurring(d.date(), st.session_state.special_holidays_list)
            if is_sp: is_holiday = True
        if is_holiday:
            holiday_cols.append(d_label)

    if st.button("シフト案を新規自動生成", type="primary"):
        rows = []
        for _, staff in shift_staff_df.iterrows():
            s_name = staff["名前"]
            row_data = {"氏名": s_name}
            for d in dates:
                d_label = f"{d.day}({jp_days[d.weekday()]})"
                wd_str = jp_days[d.weekday()]
                is_closed = False
                if d_label in holiday_cols: is_closed = True
                
                if is_closed: row_data[d_label] = "休"
                elif wd_str in staff["固定休"]: row_data[d_label] = "公休"
                else: row_data[d_label] = staff["基本シフト"]
            rows.append(row_data)
            
        new_df = pd.DataFrame(rows)
        st.session_state.current_shift_df = new_df
        save_data_to_sheet("current_shift_draft", new_df)
        st.success("新規作成しました")
        st.rerun()

    if st.session_state.current_shift_df is not None:
        current_df = st.session_state.current_shift_df
        column_config = {"氏名": st.column_config.TextColumn("氏名", disabled=True)}
        for d_col in date_cols:
            if d_col in current_df.columns:
                column_config[d_col] = st.column_config.SelectboxColumn(d_col, options=shift_opts, required=True, width="small")
        
        display_cols = ["氏名"] + [c for c in date_cols if c in current_df.columns]
        st.subheader(f"{s_year_shift}年{s_month_shift}月 シフト表")
        
        edited_df = st.data_editor(current_df[display_cols], column_config=column_config, use_container_width=True, height=400, hide_index=True, key="shift_editor_h_key")
        
        st.session_state.current_shift_df = edited_df
        save_data_to_sheet("current_shift_draft", edited_df)
        
        st.divider()
        st.subheader("👀 色付き確認")
        def highlight_holidays_col(data):
            style_df = pd.DataFrame('', index=data.index, columns=data.columns)
            for col in holiday_cols:
                if col in style_df.columns: style_df[col] = 'background-color: #ffe6e6; color: #cc0000'
            return style_df

        st.dataframe(edited_df.style.apply(highlight_holidays_col, axis=None), use_container_width=True, height=600, hide_index=True)
        csv_out = edited_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("シフト表をPCに保存 (CSV)", csv_out, "shift_h_final.csv", "text/csv")
    else:
        st.info("まだシフト表がありません。「シフト案を新規自動生成」ボタンを押してください。")
