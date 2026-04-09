"""TRNT 급여 대시보드 - 매월 급여대장/사업소득지급대장 .xls 업로드 → 통합 분석"""
import hashlib
import json
import re
import secrets
import unicodedata
from pathlib import Path
import pandas as pd
import streamlit as st
import xlrd
import plotly.express as px

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR = DATA_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "payroll.json"
EMP_PATH = DATA_DIR / "employees.json"
SMTP_PATH = DATA_DIR / "smtp.json"
AUTH_PATH = DATA_DIR / "auth.json"

COMPANIES = ["티알앤티", "트리니티 필라테스"]


# ──────────────────────────── 관리자 인증 ────────────────────────────
def _hash_pw(pw: str, salt: str) -> str:
    return hashlib.sha256((salt + pw).encode("utf-8")).hexdigest()


def load_auth():
    if AUTH_PATH.exists():
        return json.loads(AUTH_PATH.read_text(encoding="utf-8"))
    return None


def save_auth(pw: str):
    salt = secrets.token_hex(16)
    AUTH_PATH.write_text(
        json.dumps({"salt": salt, "hash": _hash_pw(pw, salt)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def require_login():
    st.set_page_config(page_title="TRNT 급여 대시보드", layout="wide")
    if st.session_state.get("authed"):
        return
    auth = load_auth()
    st.title("🔐 TRNT 급여 대시보드")

    if auth is None:
        st.info("최초 접속입니다. 관리자 비밀번호를 설정해 주세요.")
        pw1 = st.text_input("새 비밀번호", type="password", key="pw_new1")
        pw2 = st.text_input("비밀번호 확인", type="password", key="pw_new2")
        if st.button("비밀번호 설정", type="primary"):
            if not pw1 or len(pw1) < 4:
                st.error("최소 4자 이상 입력해 주세요.")
            elif pw1 != pw2:
                st.error("두 비밀번호가 일치하지 않습니다.")
            else:
                save_auth(pw1)
                st.session_state["authed"] = True
                st.success("설정 완료. 잠시 후 대시보드가 열립니다.")
                st.rerun()
        st.stop()
    else:
        pw = st.text_input("관리자 비밀번호", type="password", key="pw_login")
        col1, col2 = st.columns([1, 4])
        if col1.button("로그인", type="primary"):
            if _hash_pw(pw, auth["salt"]) == auth["hash"]:
                st.session_state["authed"] = True
                st.rerun()
            else:
                st.error("비밀번호가 맞지 않습니다.")
        with col2.expander("비밀번호를 잊으셨나요?"):
            st.caption(
                f"`{AUTH_PATH}` 파일을 삭제한 뒤 앱을 다시 로드하면 "
                "최초 설정 화면으로 돌아갑니다."
            )
        st.stop()


require_login()


# ──────────────────────────── 파일 파싱 ────────────────────────────
def detect_meta(filename: str, sheet) -> tuple[str, str, str]:
    """파일명/내용에서 (회사, 종류, 년월 'YYYY-MM') 추출"""
    name = unicodedata.normalize("NFC", filename)
    company = next((c for c in COMPANIES if c in name), "")
    if "사업소득" in name:
        kind = "사업소득"
    elif "급여대장" in name:
        kind = "급여"
    else:
        kind = ""

    ym = ""
    for r in range(min(sheet.nrows, 6)):
        for c in range(sheet.ncols):
            v = str(sheet.cell_value(r, c))
            m = re.search(r"(\d{4})\D+(\d{1,2})\s*월", v)
            if m:
                ym = f"{m.group(1)}-{int(m.group(2)):02d}"
                break
        if ym:
            break
    if not ym:
        m = re.search(r"(\d{1,2})\s*월", name)
        if m:
            ym = f"2026-{int(m.group(1)):02d}"
    return company, kind, ym


def _num(v) -> float:
    try:
        return float(v) if v not in ("", None) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _is_total(*vals) -> bool:
    """'합계', '합계 (8명)', '총계', '총   계' 등 집계행 판별."""
    for v in vals:
        s = str(v).replace(" ", "")
        if s.startswith("합계") or s.startswith("총계") or s.startswith("총합"):
            return True
    return False


def parse_payroll(sheet) -> list[dict]:
    """급여대장 파싱: 1인 = 3행.
    col 9~14 = 국민연금, 건강보험, 고용보험, 장기요양, 소득세, 지방소득세
    (r+1, 14) = 공제합계
    """
    rows = []
    r = 8
    while r < sheet.nrows:
        emp_no = str(sheet.cell_value(r, 0)).strip()
        name = str(sheet.cell_value(r, 1)).strip()
        if _is_total(emp_no, name):
            break
        if not emp_no:
            r += 1
            continue
        if r + 2 >= sheet.nrows:
            break
        gross = _num(sheet.cell_value(r + 2, 8))
        pension = _num(sheet.cell_value(r, 9))
        health = _num(sheet.cell_value(r, 10))
        employ = _num(sheet.cell_value(r, 11))
        care = _num(sheet.cell_value(r, 12))
        inc_tax = _num(sheet.cell_value(r, 13))
        local_tax = _num(sheet.cell_value(r, 14))
        deduction_total = _num(sheet.cell_value(r + 1, 14))
        if deduction_total == 0:
            deduction_total = pension + health + employ + care + inc_tax + local_tax
        if name:
            rows.append({
                "name": name,
                "gross": gross,
                "type": "급여",
                "국민연금": pension,
                "건강보험": health,
                "고용보험": employ,
                "장기요양": care,
                "소득세": inc_tax,
                "지방소득세": local_tax,
                "공제합계": deduction_total,
            })
        r += 3
    return rows


def parse_business_income(sheet) -> list[dict]:
    """사업소득지급대장 파싱: 1인 = 2행.
    (r,4)=지급액, (r,5)=소득세, (r+1,5)=지방소득세
    """
    rows = []
    r = 6
    while r < sheet.nrows:
        no = str(sheet.cell_value(r, 0)).strip()
        name = str(sheet.cell_value(r, 2)).strip()
        if _is_total(no, name):
            break
        if not no:
            r += 1
            continue
        gross = _num(sheet.cell_value(r, 4))
        inc_tax = _num(sheet.cell_value(r, 5))
        local_tax = _num(sheet.cell_value(r + 1, 5)) if r + 1 < sheet.nrows else 0.0
        if name:
            rows.append({
                "name": name,
                "gross": gross,
                "type": "사업소득",
                "국민연금": 0,
                "건강보험": 0,
                "고용보험": 0,
                "장기요양": 0,
                "소득세": inc_tax,
                "지방소득세": local_tax,
                "공제합계": inc_tax + local_tax,
            })
        r += 2
    return rows


def parse_file(filename: str, file_bytes: bytes):
    wb = xlrd.open_workbook(file_contents=file_bytes)
    sheet = wb.sheet_by_index(0)
    company, kind, ym = detect_meta(filename, sheet)
    if not (company and kind and ym):
        return None, f"메타데이터 추출 실패 (회사={company}, 종류={kind}, 년월={ym})"
    rows = parse_payroll(sheet) if kind == "급여" else parse_business_income(sheet)
    return {"company": company, "kind": kind, "ym": ym, "rows": rows}, None


# ──────────────────────────── 데이터 저장 ────────────────────────────
def dedupe(records: list[dict]) -> list[dict]:
    """(회사, 종류, 년월, 이름) 키가 같은 레코드는 뒤쪽(최근 업로드) 것만 유지."""
    seen = {}
    for r in records:
        key = (r.get("company"), r.get("kind"), r.get("ym"), r.get("name"))
        seen[key] = r  # 덮어쓰기 → 마지막 값 유지
    return list(seen.values())


def load_db() -> list[dict]:
    if DB_PATH.exists():
        return dedupe(json.loads(DB_PATH.read_text(encoding="utf-8")))
    return []


def save_db(records: list[dict]):
    records = dedupe(records)
    DB_PATH.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def load_employees() -> dict:
    """{이름: {email, memo}}"""
    if EMP_PATH.exists():
        return json.loads(EMP_PATH.read_text(encoding="utf-8"))
    return {}


def save_employees(emp: dict):
    EMP_PATH.write_text(json.dumps(emp, ensure_ascii=False, indent=2), encoding="utf-8")


def load_smtp() -> dict:
    if SMTP_PATH.exists():
        cfg = json.loads(SMTP_PATH.read_text(encoding="utf-8"))
        # 과거 admin_email 필드는 manager_email로 이관
        if "manager_email" not in cfg:
            cfg["manager_email"] = cfg.pop("admin_email", "")
        cfg.setdefault("manager_email", "")
        return cfg
    return {
        "host": "smtp.gmail.com",
        "port": 587,
        "user": "",
        "password": "",
        "sender_name": "TRNT",
        "manager_email": "",
    }


def save_smtp(cfg: dict):
    SMTP_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


TAX_FIELDS = ["국민연금", "건강보험", "고용보험", "장기요양", "소득세", "지방소득세", "공제합계"]


def upsert(records: list[dict], parsed: dict) -> list[dict]:
    """동일 (회사, 종류, 년월)는 새로 덮어씀."""
    key = (parsed["company"], parsed["kind"], parsed["ym"])
    kept = [r for r in records if (r["company"], r["kind"], r["ym"]) != key]
    for row in parsed["rows"]:
        rec = {
            "company": parsed["company"],
            "kind": parsed["kind"],
            "ym": parsed["ym"],
            "name": row["name"],
            "gross": row["gross"],
        }
        for f in TAX_FIELDS:
            rec[f] = row.get(f, 0)
        kept.append(rec)
    return kept


def to_df(records: list[dict]) -> pd.DataFrame:
    cols = ["company", "kind", "ym", "name", "gross"] + TAX_FIELDS
    if not records:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(records)
    for c in ["gross"] + TAX_FIELDS:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def won(n: float) -> str:
    return f"{int(round(n)):,}원"


# ──────────────────────────── 급여명세서 ────────────────────────────
def build_payslip_html(name: str, ym: str, items: list[dict]) -> tuple[str, str]:
    """(제목, HTML 본문) 반환. items는 같은 사람이 여러 회사에서 받은 경우를 대비해 리스트."""
    y, m = ym.split("-")
    title = f"[{int(m)}월 급여명세서] {name}님"
    total_gross = sum(it["gross"] for it in items)
    total_ded = sum(it["공제합계"] for it in items)
    total_net = total_gross - total_ded

    blocks = []
    for it in items:
        rows = [
            ("지급 (세전)", it["gross"]),
            ("국민연금", it["국민연금"]),
            ("건강보험", it["건강보험"]),
            ("고용보험", it["고용보험"]),
            ("장기요양", it["장기요양"]),
            ("소득세", it["소득세"]),
            ("지방소득세", it["지방소득세"]),
            ("공제 합계", it["공제합계"]),
            ("실지급액", it["gross"] - it["공제합계"]),
        ]
        rows_html = "".join(
            f"<tr><td style='padding:6px 12px;border-bottom:1px solid #eee'>{k}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee;text-align:right'>{int(round(v)):,}원</td></tr>"
            for k, v in rows
            if v or k in ("지급 (세전)", "공제 합계", "실지급액")
        )
        blocks.append(
            f"<h3 style='margin:16px 0 8px;color:#333'>{it['company']} · {it['kind']}</h3>"
            f"<table style='border-collapse:collapse;width:100%;font-size:14px'>{rows_html}</table>"
        )

    html = f"""<!doctype html><html><body style='font-family:-apple-system,Helvetica,sans-serif;max-width:560px;margin:0 auto;padding:24px;color:#222'>
<p style='font-size:16px;line-height:1.6'>{name}님, 안녕하세요.<br>이번달도 수고 하셨습니다. 😊</p>
<p style='font-size:14px;color:#666'>{y}년 {int(m)}월 급여명세서를 보내드립니다.</p>
<hr style='border:none;border-top:1px solid #ddd;margin:16px 0'>
{''.join(blocks)}
<hr style='border:none;border-top:2px solid #333;margin:20px 0'>
<table style='border-collapse:collapse;width:100%;font-size:15px;font-weight:bold'>
<tr><td style='padding:6px 12px'>총 지급 (세전)</td><td style='padding:6px 12px;text-align:right'>{int(round(total_gross)):,}원</td></tr>
<tr><td style='padding:6px 12px;color:#c33'>총 공제</td><td style='padding:6px 12px;text-align:right;color:#c33'>-{int(round(total_ded)):,}원</td></tr>
<tr><td style='padding:6px 12px;color:#06c;font-size:17px'>실지급액</td><td style='padding:6px 12px;text-align:right;color:#06c;font-size:17px'>{int(round(total_net)):,}원</td></tr>
</table>
<p style='margin-top:24px;font-size:13px;color:#888'>— TRNT 트리니티 필라테스 —</p>
</body></html>"""
    return title, html


def build_manager_summary_html(ym: str, mdf: pd.DataFrame) -> tuple[str, str]:
    """매니저용 집계 요약 — 개인별 금액/이름은 절대 포함하지 않음."""
    y, m = ym.split("-")
    title = f"[{y}년 {int(m)}월 급여 집계] TRNT"

    items = ["국민연금", "건강보험", "고용보험", "장기요양", "소득세", "지방소득세", "공제합계"]

    def col_vals(sub):
        return [sub["gross"].sum()] + [sub[f].sum() for f in items] + [
            sub["gross"].sum() - sub["공제합계"].sum()
        ]

    def headcount(sub):
        return sub["name"].nunique()

    rows = ["지급액 (세전)"] + items + ["지급액 (세후)"]
    trnt_df = mdf[mdf["company"] == "티알앤티"]
    trin_df = mdf[mdf["company"] == "트리니티 필라테스"]
    trnt = col_vals(trnt_df)
    trin = col_vals(trin_df)
    tot = col_vals(mdf)

    def fmt(v):
        return f"{int(round(v)):,}원"

    def td(v, bold=False, color=None):
        style = "padding:6px 12px;border-bottom:1px solid #eee;text-align:right"
        if bold:
            style += ";font-weight:bold"
        if color:
            style += f";color:{color}"
        return f"<td style='{style}'>{fmt(v)}</td>"

    header = (
        "<tr style='background:#f5f5f5'>"
        "<th style='padding:8px 12px;text-align:left;border-bottom:2px solid #333'>항목</th>"
        "<th style='padding:8px 12px;text-align:right;border-bottom:2px solid #333'>티알앤티</th>"
        "<th style='padding:8px 12px;text-align:right;border-bottom:2px solid #333'>트리니티 필라테스</th>"
        "<th style='padding:8px 12px;text-align:right;border-bottom:2px solid #333'>합계</th>"
        "</tr>"
    )
    body_rows = []
    for i, label in enumerate(rows):
        bold = label in ("지급액 (세전)", "공제합계", "지급액 (세후)")
        color = "#06c" if label == "지급액 (세후)" else None
        body_rows.append(
            f"<tr><td style='padding:6px 12px;border-bottom:1px solid #eee{';font-weight:bold' if bold else ''}'>{label}</td>"
            f"{td(trnt[i], bold, color)}{td(trin[i], bold, color)}{td(tot[i], bold, color)}</tr>"
        )
    table_html = f"<table style='border-collapse:collapse;width:100%;font-size:14px'>{header}{''.join(body_rows)}</table>"

    # 인원수만 (이름/금액 없음)
    hc_html = (
        "<table style='border-collapse:collapse;width:100%;font-size:14px;margin-top:8px'>"
        "<tr style='background:#f5f5f5'>"
        "<th style='padding:6px 12px;text-align:left;border-bottom:2px solid #333'>구분</th>"
        "<th style='padding:6px 12px;text-align:right;border-bottom:2px solid #333'>인원</th>"
        "</tr>"
        f"<tr><td style='padding:6px 12px;border-bottom:1px solid #eee'>티알앤티</td>"
        f"<td style='padding:6px 12px;border-bottom:1px solid #eee;text-align:right'>{headcount(trnt_df)}명</td></tr>"
        f"<tr><td style='padding:6px 12px;border-bottom:1px solid #eee'>트리니티 필라테스</td>"
        f"<td style='padding:6px 12px;border-bottom:1px solid #eee;text-align:right'>{headcount(trin_df)}명</td></tr>"
        f"<tr><td style='padding:6px 12px;border-bottom:1px solid #eee;font-weight:bold'>합계</td>"
        f"<td style='padding:6px 12px;border-bottom:1px solid #eee;text-align:right;font-weight:bold'>{headcount(mdf)}명</td></tr>"
        "</table>"
    )

    html = f"""<!doctype html><html><body style='font-family:-apple-system,Helvetica,sans-serif;max-width:720px;margin:0 auto;padding:24px;color:#222'>
<h2 style='margin:0 0 4px'>📊 {y}년 {int(m)}월 급여 집계 요약</h2>
<p style='color:#666;margin:0 0 20px'>TRNT (티알앤티 · 트리니티 필라테스)</p>

<h3 style='margin:20px 0 8px;border-left:4px solid #06c;padding-left:10px'>회사별 세금·공제 집계</h3>
{table_html}

<h3 style='margin:24px 0 8px;border-left:4px solid #06c;padding-left:10px'>인원 수</h3>
{hc_html}

<p style='margin-top:28px;font-size:12px;color:#888'>※ 본 요약에는 개인별 금액/명단이 포함되지 않습니다.<br>TRNT 급여 대시보드 자동 발송</p>
</body></html>"""
    return title, html


def send_email(smtp_cfg: dict, to_email, subject: str, html_body: str) -> tuple[bool, str]:
    import smtplib
    from email.mime.text import MIMEText
    from email.header import Header
    from email.utils import formataddr
    try:
        if isinstance(to_email, str):
            recipients = [e.strip() for e in to_email.split(",") if e.strip()]
        else:
            recipients = list(to_email)
        if not recipients:
            return False, "수신자 없음"
        msg = MIMEText(html_body, "html", "utf-8")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = formataddr((str(Header(smtp_cfg.get("sender_name") or "TRNT", "utf-8")), smtp_cfg["user"]))
        msg["To"] = ", ".join(recipients)
        with smtplib.SMTP(smtp_cfg["host"], int(smtp_cfg["port"])) as s:
            s.starttls()
            s.login(smtp_cfg["user"], smtp_cfg["password"])
            s.sendmail(smtp_cfg["user"], recipients, msg.as_string())
        return True, f"전송 완료 ({len(recipients)}명)"
    except Exception as e:
        return False, f"실패: {e}"


def ym_label(ym: str) -> str:
    """'2026-01' → '1월' (다년일 경우 '26년 1월')"""
    y, m = ym.split("-")
    return f"{int(m)}월"


def ordered_labels(ym_values) -> list[str]:
    uniq = sorted(set(ym_values))
    years = {v.split("-")[0] for v in uniq}
    if len(years) <= 1:
        return [f"{int(v.split('-')[1])}월" for v in uniq]
    return [f"{v.split('-')[0][2:]}년 {int(v.split('-')[1])}월" for v in uniq]


def to_label(ym: str, multi_year: bool) -> str:
    y, m = ym.split("-")
    return f"{y[2:]}년 {int(m)}월" if multi_year else f"{int(m)}월"


# ──────────────────────────── UI ────────────────────────────
header_l, header_r = st.columns([5, 1])
header_l.title("💰 TRNT 급여 대시보드")
if header_r.button("🔒 로그아웃"):
    st.session_state["authed"] = False
    st.rerun()

records = load_db()
df = to_df(records)

with st.sidebar:
    st.header("📤 파일 업로드")
    st.caption("매월 4개 파일을 한 번에 올리세요\n(급여대장 + 사업소득지급대장 × 2개사)")
    uploads = st.file_uploader(
        "엑셀(.xls) 파일 선택",
        type=["xls"],
        accept_multiple_files=True,
    )
    if uploads:
        msgs = []
        for up in uploads:
            raw = up.read()
            parsed, err = parse_file(up.name, raw)
            if err:
                msgs.append(("error", f"{up.name}: {err}"))
                continue
            records = upsert(records, parsed)
            # 원본 파일 백업: 같은 (회사·종류·월) 재업로드 시 덮어쓰기
            archive_name = f"{parsed['ym']}_{parsed['company']}_{parsed['kind']}.xls"
            (UPLOAD_DIR / archive_name).write_bytes(raw)
            msgs.append((
                "ok",
                f"{up.name} → {parsed['company']} / {parsed['kind']} / "
                f"{parsed['ym']} ({len(parsed['rows'])}명) · 저장 완료",
            ))
        save_db(records)
        df = to_df(records)
        for level, m in msgs:
            (st.success if level == "ok" else st.error)(m)

    st.divider()
    col_a, col_b = st.columns(2)
    if col_a.button("♻️ 중복 제거", type="secondary"):
        before = len(records)
        records = dedupe(records)
        save_db(records)
        st.success(f"{before - len(records)}건 제거됨")
        st.rerun()
    if col_b.button("🗑️ 전체 초기화", type="secondary"):
        save_db([])
        st.rerun()

    st.divider()
    with st.expander("🔑 관리자 비밀번호 변경"):
        old = st.text_input("현재 비밀번호", type="password", key="pw_old")
        new1 = st.text_input("새 비밀번호", type="password", key="pw_ch1")
        new2 = st.text_input("새 비밀번호 확인", type="password", key="pw_ch2")
        if st.button("변경"):
            auth = load_auth()
            if auth is None or _hash_pw(old, auth["salt"]) != auth["hash"]:
                st.error("현재 비밀번호가 맞지 않습니다.")
            elif not new1 or len(new1) < 4:
                st.error("새 비밀번호는 최소 4자 이상이어야 합니다.")
            elif new1 != new2:
                st.error("새 비밀번호가 일치하지 않습니다.")
            else:
                save_auth(new1)
                st.success("변경 완료")

if df.empty:
    st.info("좌측 사이드바에서 .xls 파일을 업로드해 주세요.")
    st.stop()

months = sorted(df["ym"].unique())

tab1, tab2, tab_emp, tab_slip, tab_hist, tab3 = st.tabs(
    ["📅 월별 현황", "📈 추이 분석", "👥 직원 관리", "📧 급여명세서", "📦 업로드 이력", "🗂 원본 데이터"]
)

# ──────────────────────────── Tab 1: 월별 현황 ────────────────────────────
with tab1:
    sel_ym = st.selectbox("조회할 월", months, index=len(months) - 1)
    mdf = df[df["ym"] == sel_ym]

    total = mdf["gross"].sum()
    by_co = mdf.groupby("company")["gross"].sum()
    trnt = by_co.get("티알앤티", 0)
    trinity = by_co.get("트리니티 필라테스", 0)
    tax_total = mdf["공제합계"].sum()
    net_total = total - tax_total

    c1, c2, c3 = st.columns(3)
    c1.metric("총 급여액 (세전)", won(total))
    c2.metric("티알앤티", won(trnt))
    c3.metric("트리니티 필라테스", won(trinity))

    c4, c5, c6 = st.columns(3)
    c4.metric("💸 세금·공제 합계", won(tax_total))
    c5.metric("실지급액 (세후)", won(net_total))
    c6.metric("원천세 (소득세+지방)", won(mdf["소득세"].sum() + mdf["지방소득세"].sum()))

    with st.expander("세금·공제 상세 내역", expanded=False):
        items = ["국민연금", "건강보험", "고용보험", "장기요양", "소득세", "지방소득세", "공제합계"]
        rows = ["지급액 (세전)"] + items + ["지급액 (세후)"]

        def col_for(sub_df):
            vals = [sub_df["gross"].sum()]
            vals += [sub_df[f].sum() for f in items]
            vals += [sub_df["gross"].sum() - sub_df["공제합계"].sum()]
            return vals

        tax_breakdown = pd.DataFrame({
            "항목": rows,
            "티알앤티": col_for(mdf[mdf["company"] == "티알앤티"]),
            "트리니티 필라테스": col_for(mdf[mdf["company"] == "트리니티 필라테스"]),
            "합계": col_for(mdf),
        })
        st.dataframe(
            tax_breakdown.style.format({
                "티알앤티": "{:,.0f}",
                "트리니티 필라테스": "{:,.0f}",
                "합계": "{:,.0f}",
            }),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("회사별 / 종류별 합계")
    pivot = (
        mdf.pivot_table(index="company", columns="kind", values="gross", aggfunc="sum", fill_value=0)
        .assign(합계=lambda x: x.sum(axis=1))
    )
    st.dataframe(
        pivot.style.format("{:,.0f}"),
        use_container_width=True,
    )

    st.subheader("👤 사람별 급여")
    person = (
        mdf.groupby(["company", "name", "kind"])[["gross", "공제합계"]].sum().reset_index()
    )
    person["실지급액"] = person["gross"] - person["공제합계"]
    person = person.sort_values(["company", "gross"], ascending=[True, False])
    person = person.rename(columns={
        "company": "회사",
        "name": "이름",
        "kind": "구분",
        "gross": "지급액 (세전)",
        "공제합계": "공제",
    })
    person[["지급액 (세전)", "공제", "실지급액"]] = person[["지급액 (세전)", "공제", "실지급액"]].astype(int)
    st.dataframe(
        person.style.format({"지급액 (세전)": "{:,}", "공제": "{:,}", "실지급액": "{:,}"}),
        use_container_width=True,
        hide_index=True,
    )

    chart_metric = st.radio(
        "그래프 기준", ["지급액 (세전)", "실지급액"], horizontal=True, key="chart_metric"
    )
    fig = px.bar(
        person,
        x="이름",
        y=chart_metric,
        color="회사",
        barmode="group",
        title=f"{sel_ym} 사람별 {chart_metric}",
    )
    fig.update_yaxes(tickformat=",.0f", ticksuffix="원", separatethousands=True)
    fig.update_traces(hovertemplate="이름=%{x}<br>" + chart_metric + "=%{y:,.0f}원<extra></extra>")
    st.plotly_chart(fig, use_container_width=True)

# ──────────────────────────── Tab 2: 추이 ────────────────────────────
with tab2:
    if len(months) < 2:
        st.info("2개월 이상의 데이터가 쌓이면 추이가 표시됩니다.")
    won_axis = dict(tickformat=",.0f", ticksuffix="원", separatethousands=True)
    multi_year = len({m.split("-")[0] for m in months}) > 1
    month_order = [to_label(m, multi_year) for m in sorted(months)]

    monthly_total = df.groupby("ym")["gross"].sum().reset_index()
    monthly_total["월"] = monthly_total["ym"].apply(lambda x: to_label(x, multi_year))
    monthly_total = monthly_total.rename(columns={"gross": "총급여"})
    fig1 = px.line(
        monthly_total, x="월", y="총급여", markers=True, title="📊 총 급여 추이",
        category_orders={"월": month_order},
    )
    fig1.update_xaxes(type="category")
    fig1.update_yaxes(**won_axis)
    fig1.update_traces(hovertemplate="월=%{x}<br>총급여=%{y:,.0f}원<extra></extra>")
    st.plotly_chart(fig1, use_container_width=True)

    monthly_co = df.groupby(["ym", "company"])["gross"].sum().reset_index()
    monthly_co["월"] = monthly_co["ym"].apply(lambda x: to_label(x, multi_year))
    monthly_co = monthly_co.rename(columns={"company": "회사", "gross": "급여"})
    fig2 = px.line(
        monthly_co, x="월", y="급여", color="회사", markers=True, title="🏢 사업장별 급여 추이",
        category_orders={"월": month_order},
    )
    fig2.update_xaxes(type="category")
    fig2.update_yaxes(**won_axis)
    fig2.update_traces(hovertemplate="월=%{x}<br>급여=%{y:,.0f}원<extra></extra>")
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("👤 사람별 급여 추이")
    co_filter = st.multiselect("회사 필터", COMPANIES, default=COMPANIES)
    pdf = df[df["company"].isin(co_filter)]
    names = sorted(pdf["name"].unique())
    name_filter = st.multiselect("이름 필터 (비우면 전체)", names)
    if name_filter:
        pdf = pdf[pdf["name"].isin(name_filter)]
    monthly_person = pdf.groupby(["ym", "name"])["gross"].sum().reset_index()
    monthly_person["월"] = monthly_person["ym"].apply(lambda x: to_label(x, multi_year))
    monthly_person = monthly_person.rename(columns={"name": "이름", "gross": "급여"})
    fig3 = px.line(
        monthly_person,
        x="월",
        y="급여",
        color="이름",
        markers=True,
        title="사람별 급여 추이",
        category_orders={"월": month_order},
    )
    fig3.update_xaxes(type="category")
    fig3.update_yaxes(**won_axis)
    fig3.update_traces(hovertemplate="이름=%{fullData.name}<br>월=%{x}<br>급여=%{y:,.0f}원<extra></extra>")
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("표: 사람 × 월")
    wide = (
        df.pivot_table(index=["company", "name"], columns="ym", values="gross", aggfunc="sum", fill_value=0)
        .reset_index()
        .rename(columns={"company": "회사", "name": "이름"})
    )
    st.dataframe(wide, use_container_width=True, hide_index=True)

# ──────────────────────────── 직원 관리 ────────────────────────────
with tab_emp:
    st.subheader("👥 직원 관리")
    st.caption("업로드된 데이터에서 자동으로 이름 목록을 만듭니다. 이메일을 입력하고 저장하세요.")

    employees = load_employees()
    all_names = sorted(df["name"].unique())

    # 누락된 이름은 빈 이메일로 채워넣기
    for n in all_names:
        employees.setdefault(n, {"email": "", "memo": ""})

    emp_df = pd.DataFrame([
        {
            "이름": n,
            "이메일": employees[n].get("email", ""),
            "메모": employees[n].get("memo", ""),
            "등록됨": bool(employees[n].get("email")),
        }
        for n in all_names
    ])

    edited = st.data_editor(
        emp_df,
        column_config={
            "이름": st.column_config.TextColumn(disabled=True),
            "이메일": st.column_config.TextColumn(help="example@domain.com"),
            "메모": st.column_config.TextColumn(),
            "등록됨": st.column_config.CheckboxColumn(disabled=True),
        },
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        key="employee_editor",
    )

    if st.button("💾 저장", type="primary"):
        new_emp = {}
        for _, row in edited.iterrows():
            new_emp[row["이름"]] = {
                "email": (row["이메일"] or "").strip(),
                "memo": (row["메모"] or "").strip(),
            }
        save_employees(new_emp)
        st.success(f"{len(new_emp)}명 저장 완료")
        st.rerun()

    with_email = sum(1 for v in employees.values() if v.get("email"))
    st.info(f"총 {len(all_names)}명 중 이메일 등록: **{with_email}명**")

# ──────────────────────────── 급여명세서 발송 ────────────────────────────
with tab_slip:
    st.subheader("📧 급여명세서 발송")

    with st.expander("✉️ SMTP 설정 (이메일 발송에 필요)", expanded=False):
        smtp_cfg = load_smtp()
        c1, c2 = st.columns(2)
        smtp_cfg["host"] = c1.text_input("SMTP 호스트", smtp_cfg.get("host", "smtp.gmail.com"))
        smtp_cfg["port"] = c2.number_input("포트", value=int(smtp_cfg.get("port", 587)), step=1)
        smtp_cfg["user"] = st.text_input("발송 계정 (이메일)", smtp_cfg.get("user", ""))
        smtp_cfg["password"] = st.text_input(
            "비밀번호 (앱 비밀번호 권장)",
            smtp_cfg.get("password", ""),
            type="password",
            help="Gmail은 '앱 비밀번호'를 발급받아 입력하세요. 네이버는 SMTP 사용 설정 후 계정 비번.",
        )
        smtp_cfg["sender_name"] = st.text_input("발신자 이름", smtp_cfg.get("sender_name", "TRNT"))
        smtp_cfg["manager_email"] = st.text_input(
            "매니저 이메일",
            smtp_cfg.get("manager_email", ""),
            help="월간 집계 요약을 받을 매니저 이메일. 여러 명은 쉼표로 구분. (개인별 금액은 포함되지 않음)",
        )
        if st.button("SMTP 설정 저장"):
            save_smtp(smtp_cfg)
            st.success("저장됨")

    sel_slip_ym = st.selectbox("발송할 월", months, index=len(months) - 1, key="slip_ym")
    mdf_slip = df[df["ym"] == sel_slip_ym]
    employees = load_employees()
    smtp_cfg = load_smtp()

    # 같은 사람이 여러 회사/종류에서 받는 경우가 있어 이름으로 그룹
    persons = sorted(mdf_slip["name"].unique())

    st.write(f"**{sel_slip_ym} 대상자: {len(persons)}명**")

    missing = [p for p in persons if not employees.get(p, {}).get("email")]
    if missing:
        st.warning(f"이메일 미등록 ({len(missing)}명): {', '.join(missing)}  →  '직원 관리' 탭에서 등록하세요.")

    # 미리보기 + 개별 발송
    for person in persons:
        items = mdf_slip[mdf_slip["name"] == person].to_dict("records")
        email = employees.get(person, {}).get("email", "")
        title, html = build_payslip_html(person, sel_slip_ym, items)
        gross = sum(it["gross"] for it in items)
        ded = sum(it["공제합계"] for it in items)

        with st.expander(f"{person}  ·  세전 {won(gross)}  ·  실지급 {won(gross - ded)}  ·  {email or '❌ 이메일 미등록'}"):
            st.components.v1.html(html, height=520, scrolling=True)
            if email and smtp_cfg.get("user") and smtp_cfg.get("password"):
                if st.button(f"📤 {person}에게 발송", key=f"send_{person}"):
                    ok, msg = send_email(smtp_cfg, email, title, html)
                    (st.success if ok else st.error)(msg)

    st.divider()
    st.markdown("### 📊 매니저에게 월간 집계 발송")
    st.caption(
        "회사별 세금·공제 합계와 인원수만 포함됩니다. "
        "**개인별 금액·이름은 절대 포함되지 않습니다.**"
    )
    manager_to = smtp_cfg.get("manager_email", "")
    if not manager_to:
        st.warning("⚠️ SMTP 설정에 '매니저 이메일'이 비어 있습니다. 위 SMTP 설정 패널에서 먼저 입력하세요.")
    else:
        st.write(f"수신: **{manager_to}**")

    with st.expander("📄 발송될 집계 미리보기"):
        _, summary_html_preview = build_manager_summary_html(sel_slip_ym, mdf_slip)
        st.components.v1.html(summary_html_preview, height=600, scrolling=True)

    can_send_manager = bool(manager_to and smtp_cfg.get("user") and smtp_cfg.get("password"))
    if st.button("📤 매니저에게 월간 집계 발송", type="primary", disabled=not can_send_manager, key="send_manager_summary"):
        title, summary_html = build_manager_summary_html(sel_slip_ym, mdf_slip)
        ok, msg = send_email(smtp_cfg, manager_to, title, summary_html)
        (st.success if ok else st.error)(msg)

    st.divider()
    col_a, col_b = st.columns([1, 2])
    if col_a.button("📤 전체 발송", type="primary", disabled=not (smtp_cfg.get("user") and smtp_cfg.get("password"))):
        prog = st.progress(0.0)
        log = []
        sendable = [p for p in persons if employees.get(p, {}).get("email")]
        for i, p in enumerate(sendable, 1):
            items = mdf_slip[mdf_slip["name"] == p].to_dict("records")
            title, html = build_payslip_html(p, sel_slip_ym, items)
            ok, msg = send_email(smtp_cfg, employees[p]["email"], title, html)
            log.append(f"{'✅' if ok else '❌'} {p}: {msg}")
            prog.progress(i / len(sendable))
        col_b.write("\n\n".join(log))
    if not (smtp_cfg.get("user") and smtp_cfg.get("password")):
        st.caption("SMTP 설정(위 확장 패널)을 저장하면 발송 버튼이 활성화됩니다.")

# ──────────────────────────── 업로드 이력 ────────────────────────────
with tab_hist:
    st.subheader("📦 업로드 이력 & 저장된 원본 파일")
    st.caption(
        "업로드한 원본 .xls 파일과 파싱 결과가 모두 로컬에 저장됩니다. "
        "앱을 재시작하거나 브라우저를 닫아도 계속 보입니다."
    )

    # 저장된 레코드 요약 (월 × 회사 × 종류)
    summary = (
        df.groupby(["ym", "company", "kind"])
        .agg(인원=("name", "nunique"), 세전합계=("gross", "sum"))
        .reset_index()
        .sort_values(["ym", "company", "kind"])
    )
    summary["세전합계"] = summary["세전합계"].astype(int)
    summary = summary.rename(columns={"ym": "월", "company": "회사", "kind": "종류"})

    st.markdown("#### 저장된 데이터")
    if summary.empty:
        st.info("저장된 데이터가 없습니다.")
    else:
        st.dataframe(
            summary.style.format({"세전합계": "{:,}"}),
            use_container_width=True,
            hide_index=True,
        )

    # 원본 파일 목록 & 다운로드
    st.markdown("#### 백업된 원본 .xls 파일")
    files = sorted(UPLOAD_DIR.glob("*.xls"))
    if not files:
        st.info("백업된 원본 파일이 없습니다. 앞으로 업로드되는 파일부터 자동 저장됩니다.")
    else:
        for f in files:
            cols = st.columns([4, 1, 1, 1])
            size_kb = f.stat().st_size / 1024
            mtime = pd.Timestamp(f.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M")
            cols[0].write(f"📄 **{f.name}**")
            cols[1].caption(f"{size_kb:,.0f} KB")
            cols[2].caption(mtime)
            cols[3].download_button(
                "⬇️",
                f.read_bytes(),
                file_name=f.name,
                mime="application/vnd.ms-excel",
                key=f"dl_{f.name}",
            )

    # 월별 삭제
    st.markdown("#### 특정 월 삭제")
    if months:
        del_ym = st.selectbox("삭제할 월", months, key="del_ym")
        if st.button(f"🗑️ {del_ym} 데이터 전체 삭제", type="secondary"):
            records = [r for r in records if r["ym"] != del_ym]
            save_db(records)
            for f in UPLOAD_DIR.glob(f"{del_ym}_*.xls"):
                f.unlink()
            st.success(f"{del_ym} 데이터·원본 파일 삭제 완료")
            st.rerun()

# ──────────────────────────── Tab 3: 원본 ────────────────────────────
with tab3:
    st.dataframe(
        df.sort_values(["ym", "company", "kind", "name"]),
        use_container_width=True,
        hide_index=True,
    )
    st.download_button(
        "CSV 다운로드",
        df.to_csv(index=False).encode("utf-8-sig"),
        file_name="payroll_all.csv",
        mime="text/csv",
    )
