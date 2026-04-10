import io
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from supabase import create_client

st.set_page_config(page_title="供应商数据下载", layout="wide")

# ========= Supabase 连接 =========
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= 配置 =========
BUCKET_NAME = "archive-files"
RETENTION_DAYS = 30

# ========= 时区：统一北京时间 =========
BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def now_bj():
    return datetime.now(BEIJING_TZ)


def now_bj_str():
    return now_bj().strftime("%Y-%m-%d %H:%M:%S")


def today_bj():
    return now_bj().date()


# ========= 工具函数 =========
def safe_filename(name: str) -> str:
    name = str(name).strip() if name is not None else "未填写运输"
    if not name:
        name = "未填写运输"
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    name = re.sub(r"\s+", "_", name)
    return name


def extract_date_from_filename(filename: str) -> str:
    """
    从文件名里提取日期：
    支持 20260407 / 2026-04-07 / 2026_04_07
    返回 YYYYMMDD
    """
    if not filename:
        return now_bj().strftime("%Y%m%d")

    base = os.path.splitext(os.path.basename(filename))[0]

    m = re.search(r"(20\d{6})", base)
    if m:
        return m.group(1)

    m = re.search(r"(20\d{2})[-_](\d{2})[-_](\d{2})", base)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"

    return now_bj().strftime("%Y%m%d")


def normalize_date_display(yyyymmdd: str) -> str:
    if len(yyyymmdd) == 8 and yyyymmdd.isdigit():
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
    return yyyymmdd


def get_admin_password() -> str:
    try:
        return st.secrets["ADMIN_PASSWORD"]
    except Exception:
        return "admin123"


def format_db_time_to_bj_str(value):
    """
    把数据库返回的时间统一转成北京时间字符串
    """
    if value is None:
        return ""

    text = str(value).strip()
    if not text:
        return ""

    try:
        text = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BEIJING_TZ)
        dt_bj = dt.astimezone(BEIJING_TZ)
        return dt_bj.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return text


def build_storage_path(source_name: str, source_date: str) -> str:
    """
    生成只包含英文/数字的 Storage 路径，避免中文文件名导致 InvalidKey
    """
    ext = os.path.splitext(source_name)[1].lower()
    if ext not in [".xlsx", ".xlsm"]:
        ext = ".xlsx"

    timestamp = now_bj().strftime("%Y%m%d_%H%M%S")
    return f"archive/{source_date}_file_{timestamp}{ext}"


# ========= 供应商密码：Supabase =========
def load_passwords() -> dict:
    result = supabase.table("supplier_passwords").select("supplier_name,password").execute()

    data = {}
    for row in (result.data or []):
        supplier_name = str(row.get("supplier_name", "")).strip()
        password = str(row.get("password", ""))
        if supplier_name:
            data[supplier_name] = password

    return data


def save_passwords(data: dict):
    for supplier_name, password in data.items():
        supplier_name = str(supplier_name).strip()
        password = str(password).strip()

        if supplier_name:
            supabase.table("supplier_passwords").upsert({
                "supplier_name": supplier_name,
                "password": password,
                "updated_at": now_bj().isoformat()
            }).execute()


# ========= 下载日志：Supabase =========
def load_download_logs() -> list:
    result = (
        supabase
        .table("download_logs")
        .select("*")
        .order("download_time", desc=True)
        .execute()
    )

    logs = result.data or []

    for row in logs:
        row["download_time"] = format_db_time_to_bj_str(row.get("download_time"))

    return logs


def log_download_event(supplier_name: str, source_date: str, source_name: str, download_name: str, row_count: int):
    supabase.table("download_logs").insert({
        "download_time": now_bj().isoformat(),
        "supplier_name": supplier_name,
        "source_date": source_date,
        "source_name": source_name,
        "download_name": download_name,
        "row_count": int(row_count),
    }).execute()


# ========= 归档文件：Supabase Storage + archive_files 表 =========
def cleanup_old_files(retention_days: int = RETENTION_DAYS):
    """
    清理超过保留天数的文件：
    - 删除 Storage 里的文件
    - 把 archive_files 表中的记录标记为 is_deleted = true
    """
    result = (
        supabase
        .table("archive_files")
        .select("*")
        .eq("is_deleted", False)
        .execute()
    )

    records = result.data or []
    today = today_bj()

    for rec in records:
        source_date = str(rec.get("source_date", "")).strip()
        storage_path = str(rec.get("storage_path", "")).strip()

        try:
            file_date = datetime.strptime(source_date, "%Y%m%d").date()
            if (today - file_date).days > retention_days:
                if storage_path:
                    try:
                        supabase.storage.from_(BUCKET_NAME).remove([storage_path])
                    except Exception:
                        pass

                supabase.table("archive_files").update({
                    "is_deleted": True
                }).eq("source_date", source_date).execute()
        except Exception:
            continue


def save_uploaded_file(uploaded_file):
    """
    上传文件到 Supabase Storage，并在 archive_files 表中写入索引
    同一天重复上传时，覆盖当天旧记录
    """
    cleanup_old_files()

    source_name = uploaded_file.name
    source_date = extract_date_from_filename(source_name)
    storage_path = build_storage_path(source_name, source_date)

    # 根据扩展名给 content-type
    ext = os.path.splitext(source_name)[1].lower()
    if ext == ".xlsm":
        content_type = "application/vnd.ms-excel.sheet.macroEnabled.12"
    else:
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    # 先看看同一天是否已有旧记录
    existing = (
        supabase
        .table("archive_files")
        .select("*")
        .eq("source_date", source_date)
        .limit(1)
        .execute()
    )

    existing_rows = existing.data or []
    if existing_rows:
        old_storage_path = str(existing_rows[0].get("storage_path", "")).strip()
        if old_storage_path:
            try:
                supabase.storage.from_(BUCKET_NAME).remove([old_storage_path])
            except Exception:
                pass

    # 关键：上传 bytes，不直接传 UploadedFile
    file_bytes = uploaded_file.getvalue()

    supabase.storage.from_(BUCKET_NAME).upload(
        path=storage_path,
        file=file_bytes,
        file_options={
            "upsert": "true",
            "content-type": content_type
        }
    )

    # 写入索引表
    supabase.table("archive_files").upsert({
        "source_date": source_date,
        "source_name": source_name,
        "upload_time": now_bj().isoformat(),
        "storage_path": storage_path,
        "is_deleted": False
    }).execute()


def delete_record_by_date(source_date: str):
    """
    删除指定日期的归档文件：
    - 删除 Storage 文件
    - 将 archive_files 标记为 is_deleted = true
    """
    record = (
        supabase
        .table("archive_files")
        .select("*")
        .eq("source_date", source_date)
        .eq("is_deleted", False)
        .limit(1)
        .execute()
    )

    rows = record.data or []
    if not rows:
        return

    storage_path = str(rows[0].get("storage_path", "")).strip()

    if storage_path:
        try:
            supabase.storage.from_(BUCKET_NAME).remove([storage_path])
        except Exception:
            pass

    supabase.table("archive_files").update({
        "is_deleted": True
    }).eq("source_date", source_date).execute()


def get_archive_records() -> list:
    cleanup_old_files()

    result = (
        supabase
        .table("archive_files")
        .select("*")
        .eq("is_deleted", False)
        .order("source_date", desc=True)
        .execute()
    )

    records = result.data or []
    return records


def get_record_by_date(source_date: str):
    result = (
        supabase
        .table("archive_files")
        .select("*")
        .eq("source_date", source_date)
        .eq("is_deleted", False)
        .limit(1)
        .execute()
    )

    rows = result.data or []
    return rows[0] if rows else None


def load_df_from_record(record):
    """
    从 Supabase Storage 下载 Excel，并读取成 DataFrame
    """
    if not record:
        return None, None

    storage_path = str(record.get("storage_path", "")).strip()
    if not storage_path:
        return None, None

    file_bytes = supabase.storage.from_(BUCKET_NAME).download(storage_path)
    df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    df = df.fillna("")
    transport_col = find_transport_column(df)
    return df, transport_col


def find_transport_column(df: pd.DataFrame):
    """
    优先找表头“运输”
    找不到就退回 AN 列（第40列，索引39）
    """
    for col in df.columns:
        if str(col).strip() == "运输":
            return col

    if len(df.columns) >= 40:
        return df.columns[39]

    raise ValueError("找不到“运输”列，也没有 AN 列可用。")


def get_supplier_list(df: pd.DataFrame, transport_col):
    values = df[transport_col].astype(str).str.strip()
    suppliers = [x for x in values.unique().tolist() if x]
    suppliers.sort()
    return suppliers


def get_all_suppliers_from_all_records(records, passwords=None):
    """
    统计所有已归档表格里出现过的全部供应商
    同时把已经设置过密码的供应商也并入，避免漏掉
    """
    supplier_set = set()

    if passwords:
        for name in passwords.keys():
            if str(name).strip():
                supplier_set.add(str(name).strip())

    for rec in records:
        try:
            df, transport_col = load_df_from_record(rec)
            if df is not None:
                suppliers = get_supplier_list(df, transport_col)
                for s in suppliers:
                    if str(s).strip():
                        supplier_set.add(str(s).strip())
        except Exception:
            continue

    result = list(supplier_set)
    result.sort()
    return result


def get_records_for_supplier(records, supplier_name: str):
    """
    返回某个供应商在哪些日期有数据
    """
    matched = []

    for rec in records:
        try:
            df, transport_col = load_df_from_record(rec)
            if df is None:
                continue

            supplier_df = df[df[transport_col].astype(str).str.strip() == supplier_name]
            if not supplier_df.empty:
                matched.append(rec)
        except Exception:
            continue

    matched.sort(key=lambda x: x.get("source_date", ""), reverse=True)
    return matched


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="数据")
    output.seek(0)
    return output.getvalue()


# ========= 页面标题 =========
st.title("供应商数据下载网页")
st.caption("管理员上传总表后，系统按文件名日期归档；供应商可输入名称、密码，并按日期查看和下载自己的数据。")


# ========= 初始化状态 =========
if "admin_ok" not in st.session_state:
    st.session_state.admin_ok = False

if "viewer_ok" not in st.session_state:
    st.session_state.viewer_ok = False

if "viewer_supplier" not in st.session_state:
    st.session_state.viewer_supplier = ""

if "delete_target_date" not in st.session_state:
    st.session_state.delete_target_date = None


# ========= 侧边栏 =========
with st.sidebar:
    st.header("使用说明")
    st.write("1. 管理员上传当天总表")
    st.write("2. 系统按文件名日期归档")
    st.write("3. 默认保留最近 30 天")
    st.write("4. 管理员可为所有历史表格出现过的供应商设置密码")
    st.write("5. 供应商输入自己的供应商名称和密码")
    st.write("6. 再按日期查看和下载自己的数据")
    st.write("7. 管理员可查看供应商下载日志")
    st.info("所有显示时间已统一为北京时间")
    st.info("当前版本：密码、日志、文件归档、归档索引都已迁到 Supabase")


# ========= 标签页 =========
tab_admin, tab_supplier = st.tabs(["管理员区", "供应商下载区"])


# ========= 管理员区 =========
with tab_admin:
    st.subheader("管理员登录")

    admin_pwd_input = st.text_input("请输入管理员密码", type="password", key="admin_pwd_input")

    if st.button("进入管理员区"):
        if admin_pwd_input == get_admin_password():
            st.session_state.admin_ok = True
            st.success("管理员登录成功")
        else:
            st.session_state.admin_ok = False
            st.error("管理员密码错误")

    if st.session_state.admin_ok:
        st.markdown("---")
        st.subheader("1）上传当天总表")

        uploaded = st.file_uploader("请选择 Excel 文件", type=["xlsx", "xlsm"])

        if uploaded is not None:
            source_date = extract_date_from_filename(uploaded.name)
            st.write(f"已选择文件：{uploaded.name}")
            st.write(f"识别到日期：{normalize_date_display(source_date)}")
            st.write("说明：如果该日期已存在旧文件，上传后会覆盖该日期的旧版本。")

            if st.button("保存并归档"):
                save_uploaded_file(uploaded)
                st.success("文件已归档成功。")
                st.rerun()

        st.markdown("---")
        st.subheader("2）设置供应商密码")

        records = get_archive_records()
        passwords = load_passwords()
        all_suppliers = get_all_suppliers_from_all_records(records, passwords)

        if all_suppliers:
            st.write(f"当前可设置密码的供应商数量：{len(all_suppliers)}")

            selected_for_pwd = st.selectbox("选择要设置密码的供应商", all_suppliers)
            new_password = st.text_input("输入该供应商的新密码", type="password", key="new_supplier_password")

            if st.button("保存这个供应商的密码"):
                if not new_password.strip():
                    st.warning("密码不能为空")
                else:
                    passwords[selected_for_pwd] = new_password.strip()
                    save_passwords(passwords)
                    st.success(f"已保存：{selected_for_pwd}")
                    st.rerun()

            st.markdown("**已设置密码的供应商**")
            names = list(passwords.keys())
            names.sort()
            if names:
                st.write("、".join(names))
            else:
                st.write("还没有设置任何供应商密码。")
        else:
            st.info("请先上传至少一份总表。")

        st.markdown("---")
        st.subheader("3）已归档文件")

        records = get_archive_records()
        if records:
            for rec in records:
                date_raw = str(rec.get("source_date", ""))
                date_text = normalize_date_display(date_raw)
                source_name = str(rec.get("source_name", ""))
                upload_time = format_db_time_to_bj_str(rec.get("upload_time"))

                row_col1, row_col2 = st.columns([11, 2], gap="small")
                with row_col1:
                    st.markdown(
                        f"**{date_text}**　|　{source_name}　|　上传时间：{upload_time}"
                    )
                with row_col2:
                    if st.button("删除", key=f"delete_btn_{date_raw}"):
                        st.session_state.delete_target_date = date_raw
                        st.rerun()

                if st.session_state.delete_target_date == date_raw:
                    confirm_col1, confirm_col2, confirm_col3 = st.columns([8, 2, 2], gap="small")
                    with confirm_col1:
                        st.warning(f"确认删除 {date_text} 对应文件？删除后无法恢复。")
                    with confirm_col2:
                        if st.button("确认", key=f"confirm_delete_{date_raw}"):
                            delete_record_by_date(date_raw)
                            st.session_state.delete_target_date = None
                            st.success("删除成功。")
                            st.rerun()
                    with confirm_col3:
                        if st.button("取消", key=f"cancel_delete_{date_raw}"):
                            st.session_state.delete_target_date = None
                            st.rerun()
        else:
            st.write("当前没有归档文件。")

        st.markdown("---")
        st.subheader("4）下载日志")

        logs = load_download_logs()
        if logs:
            log_df = pd.DataFrame(logs)

            supplier_options = ["全部"] + sorted(log_df["supplier_name"].dropna().astype(str).unique().tolist())
            date_options = ["全部"] + sorted(log_df["source_date"].dropna().astype(str).unique().tolist(), reverse=True)

            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                selected_log_supplier = st.selectbox("按供应商筛选", supplier_options, key="log_supplier_filter")
            with filter_col2:
                selected_log_date = st.selectbox(
                    "按文件日期筛选",
                    date_options,
                    format_func=lambda x: "全部" if x == "全部" else normalize_date_display(x),
                    key="log_date_filter"
                )

            filtered_df = log_df.copy()

            if selected_log_supplier != "全部":
                filtered_df = filtered_df[filtered_df["supplier_name"] == selected_log_supplier]

            if selected_log_date != "全部":
                filtered_df = filtered_df[filtered_df["source_date"] == selected_log_date]

            filtered_df = filtered_df.copy()
            filtered_df["source_date_display"] = filtered_df["source_date"].apply(normalize_date_display)

            display_df = filtered_df[[
                "download_time",
                "supplier_name",
                "source_date_display",
                "source_name",
                "download_name",
                "row_count"
            ]].rename(columns={
                "download_time": "下载时间",
                "supplier_name": "供应商",
                "source_date_display": "文件日期",
                "source_name": "原文件名",
                "download_name": "下载文件名",
                "row_count": "数据行数"
            })

            st.write(f"当前共有 {len(display_df)} 条下载记录")
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.write("当前还没有下载日志。")


# ========= 供应商下载区 =========
with tab_supplier:
    st.subheader("供应商登录并下载")

    records = get_archive_records()

    if not records:
        st.warning("管理员还没有上传任何总表。")
    else:
        passwords = load_passwords()

        input_supplier = st.text_input("请输入你的供应商名称").strip()
        supplier_pwd_input = st.text_input("请输入密码", type="password", key="supplier_pwd_input")

        if input_supplier != st.session_state.viewer_supplier:
            st.session_state.viewer_ok = False
            st.session_state.viewer_supplier = input_supplier

        if st.button("验证身份"):
            if not input_supplier:
                st.error("请输入供应商名称。")
                st.session_state.viewer_ok = False
            else:
                real_pwd = passwords.get(input_supplier)

                if real_pwd is None:
                    st.error("供应商名称不存在，或管理员还没有为该供应商设置密码。")
                    st.session_state.viewer_ok = False
                elif supplier_pwd_input != real_pwd:
                    st.error("密码错误。")
                    st.session_state.viewer_ok = False
                else:
                    st.session_state.viewer_ok = True
                    st.success("验证成功。")

        if st.session_state.viewer_ok and input_supplier:
            supplier_records = get_records_for_supplier(records, input_supplier)

            if not supplier_records:
                st.warning("当前所有归档文件中都没有找到你的数据。")
            else:
                date_options = [rec["source_date"] for rec in supplier_records]
                date_labels = {d: normalize_date_display(d) for d in date_options}

                selected_date = st.selectbox(
                    "请选择日期",
                    options=date_options,
                    format_func=lambda x: date_labels.get(x, x)
                )

                record = get_record_by_date(selected_date)
                if record:
                    st.write(f"当前日期文件：{record.get('source_name', '未知文件')}")

                    try:
                        df, transport_col = load_df_from_record(record)

                        if df is None:
                            st.error("该日期文件读取失败。")
                        else:
                            supplier_df = df[df[transport_col].astype(str).str.strip() == input_supplier].copy()

                            if supplier_df.empty:
                                st.warning("该日期下没有你的数据。")
                            else:
                                st.write(f"当前共有 {len(supplier_df)} 条数据")
                                st.dataframe(supplier_df, use_container_width=True, hide_index=True)

                                download_name = f"{selected_date}_{safe_filename(input_supplier)}.xlsx"
                                excel_bytes = dataframe_to_excel_bytes(supplier_df)

                                st.download_button(
                                    label="下载我的 Excel",
                                    data=excel_bytes,
                                    file_name=download_name,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"download_btn_{selected_date}_{safe_filename(input_supplier)}",
                                    on_click=log_download_event,
                                    args=(
                                        input_supplier,
                                        selected_date,
                                        record.get("source_name", ""),
                                        download_name,
                                        len(supplier_df)
                                    )
                                )
                    except Exception as e:
                        st.error(f"读取数据失败：{e}")
