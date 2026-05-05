"""
TWI PDF Rendering Engine  —  FastAPI
=====================================
Pure PDF engine — no CRM credentials needed.
Deluge sends record data, engine returns filled PDF as base64.
Deluge attaches it to the CRM record natively.

Endpoints
---------
POST /generate       Fill TWI enrolment form PDF
POST /datasheet      Generate raw data verification sheet
POST /studentsheet   Generate Blastline Institute Student Data Sheet
GET  /pdf/{job_id}   Download PDF by job ID
GET  /queue          Job log (Basic Auth)
GET  /fields         List all PDF form field names
GET  /health         Server status

Run
---
    uvicorn render_engine:app --host 0.0.0.0 --port 8504
"""

import base64, io, json, os, re, textwrap, uuid
from collections import OrderedDict
from datetime import datetime, date
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, Image as RLImage,
)
from reportlab.platypus.flowables import Flowable
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

from pypdf import PdfReader, PdfWriter
from pypdf.generic import (
    BooleanObject, DecodedStreamObject, DictionaryObject,
    NameObject, RectangleObject,
)

# ══════════════════════════════════════════════════════════════════════════════
# PATHS
# ══════════════════════════════════════════════════════════════════════════════
_APP_DIR          = Path(__file__).parent
DATA_DIR          = Path(os.environ.get("DATA_DIR", str(_APP_DIR)))
PDF_TEMPLATE_PATH = _APP_DIR / "enrolment-form-india.pdf"
LOGO_PATH         = _APP_DIR / "logo_institute.png"
JOBS_FILE         = DATA_DIR / "render_jobs.json"
PDF_STORE         = DATA_DIR / "rendered_pdfs"
PDF_STORE.mkdir(parents=True, exist_ok=True)

app      = FastAPI(title="TWI PDF Rendering Engine")
security = HTTPBasic()

QUEUE_USER = os.environ.get("QUEUE_USER", "blastline").encode()
QUEUE_PASS = os.environ.get("QUEUE_PASS", "TWI@2026").encode()

def require_auth(credentials: HTTPBasicCredentials = Depends(security)):
    user_ok = secrets.compare_digest(credentials.username.encode(), QUEUE_USER)
    pass_ok = secrets.compare_digest(credentials.password.encode(), QUEUE_PASS)
    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=401, detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

app.add_middleware(
    CORSMiddleware, allow_origins=["*"],
    allow_methods=["*"], allow_headers=["*"],
)

# ══════════════════════════════════════════════════════════════════════════════
# FIELD METADATA  (TWI form)
# ══════════════════════════════════════════════════════════════════════════════

COMB_FIELDS   = {"undefined": 6, "D": 2, "M": 2, "Y": 4}
SPECIAL_TOKENS = {"__dob__", "__ignore__"}

AUTO_MAP_RULES = [
    (r"batch.?date|exam.?date",                                    "Event date"),
    (r"course.?name|event.?title",                                 "Event title"),
    (r"candidate.?name|name.?as.?per.?id",                         "Candidates Family Name as per ID  Passport"),
    (r"twi.?candidate.?(number|id|no)",                            "undefined"),
    (r"date.?of.?birth|dob",                                       "__dob__"),
    (r"^address$|address.?line.?1|permanent.?address",             "Permanent private address 1"),
    (r"^city$|address.?line.?2",                                   "Permanent private address 2"),
    (r"^district$|address.?line.?3",                               "Permanent private address 3"),
    (r"sponsoring.*pincode",                                       "Postcode_2"),
    (r"pincode|postcode|postal",                                   "Postcode"),
    (r"correspondence.*1|correspondence.?address$",                "Correspondence address if different from above 1"),
    (r"correspondence.*2",                                         "Correspondence address if different from above 2"),
    (r"correspondence.*3",                                         "Correspondence address if different from above 3"),
    (r"correspondence.*4",                                         "Correspondence address if different from above 4"),
    (r"invoice.*1|invoice.?address$",                              "Invoice address if different from below 1"),
    (r"invoice.*2",                                                "Invoice address if different from below 2"),
    (r"invoice.*3",                                                "Invoice address if different from below 3"),
    (r"invoice.*4",                                                "Invoice address if different from below 4"),
    (r"sponsoring.*address.*1|sponsoring.*1",                      "Sponsoring Company and Address 1"),
    (r"sponsoring.*address.*2|sponsoring.*2",                      "Sponsoring Company and Address 2"),
    (r"sponsoring.*address.*3|sponsoring.*3",                      "Sponsoring Company and Address 3"),
    (r"^contact.?name$",                                           "Contact Name"),
    (r"contact.?tel(ephone)?",                                     "Tel_2"),
    (r"contact.?email",                                            "Email_2"),
    (r"contact.?no|mobile|private.?tel",                           "Private Tel"),
    (r"emergency.?contact",                                        "Tel"),
    (r"^email$|candidate.?email",                                  "Email"),
    (r"pcn.*bgas.*approval|bgas.*approval|pcn.*approval",          "PCN or BGAS Approval Number"),
    (r"bgas.?cert|pcn.?cert|bgas.?no",                             "PCN or BGAS Approval Number"),
    (r"cswip.*cert|cswip.*no|cswip.*qualif|current.*cswip",        "Current CSWIP qualifications held"),
    (r"duties|responsibilities",                                   "1"),
    (r"section.?5.?detail|detailed.?statement",                    "1_2"),
    (r"ndt.*exp|plant.*exp",                                       "1_2"),
    (r"company.?name|present.?company|sponsor.*company",           "Sponsoring Company and Address 1"),
    (r"company.*order|order.*no",                                  "Company order No"),
    (r"approving.*manager|manager.*name",                          "name"),
    (r"verifier.*professional|professional.*relation",             "to the candidate"),
    (r"verifier.*company.*pos|verifier.*position",                 "Company  position"),
    (r"verifier.?name",                                            "Name in capitals"),
    (r"verifier.?phone|verifier.?tel",                             "Telephone no"),
    (r"verifier.?email",                                           "Email Address"),
    (r"verified.?date",                                            "Date"),
    # Datasheet-only fields
    (r"sslc.?year|sslc",                                           "__ignore__"),
    (r"degree.?year|diploma.?year",                                "__ignore__"),
    (r"current.?designation",                                      "__ignore__"),
    (r"current.?job.?started|job.?started",                        "__ignore__"),
    (r"total.?years|years.?of.?exp",                               "__ignore__"),
    (r"^state$",                                                   "__ignore__"),
    (r"whatsapp",                                                  "__ignore__"),
    (r"^venue$",                                                   "__ignore__"),
    (r"cswip.?3.?1.?cert",                                         "__ignore__"),
    # Catch-all
    (r"designation|company.*position",                             "Company  position"),
]

# ══════════════════════════════════════════════════════════════════════════════
# PDF FIELD SCANNER
# ══════════════════════════════════════════════════════════════════════════════

_pdf_fields_cache: dict = {}

def get_pdf_fields() -> dict:
    global _pdf_fields_cache
    if _pdf_fields_cache:
        return _pdf_fields_cache
    if not PDF_TEMPLATE_PATH.exists():
        return {}
    reader = PdfReader(str(PDF_TEMPLATE_PATH))
    result = {}
    for pg_idx, page in enumerate(reader.pages):
        for ref in page.get("/Annots", []):
            try:   annot = ref.get_object()
            except: continue
            if not annot or annot.get("/Subtype") != "/Widget":
                continue
            name = str(annot.get("/T", "")).strip()
            if not name:
                continue
            ft = str(annot.get("/FT", ""))
            ff = annot.get("/Ff", 0)
            try:   ff = int(ff)
            except: ff = 0
            rect = [float(v) for v in annot.get("/Rect", [0,0,0,0])]
            result[name] = {
                "page":      pg_idx,
                "type":      ft,
                "comb":      bool(ff & (1 << 24)),
                "multiline": bool(ff & (1 << 12)),
                "w":         round(rect[2] - rect[0], 1),
                "h":         round(rect[3] - rect[1], 1),
            }
    _pdf_fields_cache = result
    return result

# ══════════════════════════════════════════════════════════════════════════════
# MAPPING  (TWI form)
# ══════════════════════════════════════════════════════════════════════════════

def load_manual_mappings() -> dict:
    mf = DATA_DIR / "manual_mappings.json"
    if mf.exists():
        try:   return json.loads(mf.read_text())
        except: pass
    return {}

def _split_dob(s: str):
    s = str(s).strip()
    s = re.sub(r"T\d{2}:\d{2}.*$", "", s).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y",
                "%d-%b-%Y", "%b %d, %Y", "%B %d, %Y",
                "%d %b %Y", "%d %B %Y"):
        try:
            d = datetime.strptime(s[:12], fmt)
            return str(d.day).zfill(2), str(d.month).zfill(2), str(d.year)
        except ValueError:
            continue
    digits = re.sub(r"\D", "", s)
    if len(digits) == 8:
        return digits[6:8], digits[4:6], digits[0:4]
    return "", "", ""

def apply_mappings(raw: dict) -> dict:
    pdf_fields = get_pdf_fields()
    pdf_text   = {k for k,v in pdf_fields.items() if v["type"] == "/Tx"}
    manual     = load_manual_mappings()
    auto = {}
    for zk in raw:
        matched = None
        for pattern, target in AUTO_MAP_RULES:
            if re.search(pattern, zk, re.IGNORECASE):
                matched = target
                break
        if matched is None:
            norm_zk = re.sub(r"[^a-z0-9]", "", zk.lower())
            for pf in pdf_text:
                norm_pf = re.sub(r"[^a-z0-9]", "", pf.lower())
                if norm_zk and norm_pf and len(norm_pf) >= 3 and (norm_zk in norm_pf or norm_pf in norm_zk):
                    matched = pf
                    break
        auto[zk] = matched or "__ignore__"
    effective = {**auto, **{k: v for k, v in manual.items() if k in raw}}
    out = {}
    for k, v in raw.items():
        target = effective.get(k, "__ignore__")
        if target == "__ignore__":
            continue
        if target == "__dob__":
            dd, dm, dy = _split_dob(str(v))
            print(f"[DOB] raw='{v}' → D='{dd}' M='{dm}' Y='{dy}'")
            out["D"] = dd; out["M"] = dm; out["Y"] = dy
        else:
            out[target] = str(v) if v is not None else ""
    return out

def build_field_values(mapped: dict) -> dict:
    pdf_fields = get_pdf_fields()
    pdf_text   = {k for k,v in pdf_fields.items() if v["type"] == "/Tx"}
    NO_CAPS = {"Email", "Email_2", "Email Address", "Date", "Event date"}
    fv = {
        k: (str(v) if k in NO_CAPS else str(v).upper())
        for k, v in mapped.items()
        if k in pdf_text and v is not None
    }
    if not fv.get("Date"):
        fv["Date"] = date.today().strftime("%d/%m/%Y")
    return fv

# ══════════════════════════════════════════════════════════════════════════════
# PDF FILLER  (TWI form)
# ══════════════════════════════════════════════════════════════════════════════

def _build_comb_ap(value: str, max_len: int, rect: tuple, fs: float = 8.0) -> bytes:
    x1, y1, x2, y2 = rect
    cell_w = (x2 - x1) / max_len
    base   = (y2 - y1 - fs) / 2.0 + 1.0
    lines  = ["q", "BT", f"/Cour {fs} Tf", "0 0 0 rg"]
    val    = str(value).ljust(max_len)[:max_len]
    for i, ch in enumerate(val):
        if not ch.strip(): continue
        cx   = i * cell_w + (cell_w - fs * 0.6) / 2.0
        safe = ch.replace("\\","\\\\").replace("(","\\(").replace(")","\\)")
        lines.append(f"1 0 0 1 {cx:.2f} {base:.2f} Tm")
        lines.append(f"({safe}) Tj")
    lines += ["ET", "Q"]
    return "\n".join(lines).encode()

def _build_multiline_ap(value: str, rect: tuple) -> bytes:
    x1, y1, x2, y2 = rect
    w, h = x2 - x1, y2 - y1
    PAD_TOP = 2.0; PAD_BOT = 9.0; LG = 1.3
    MAX_FS  = 9.0; MIN_FS  = 6.0; CPP = 0.52
    def wrap(text, fs):
        mc = max(1, int(w / (fs * CPP)))
        lines = []
        for para in text.replace("\r\n","\n").replace("\r","\n").split("\n"):
            lines.extend(textwrap.wrap(para, width=mc) or [""])
        return lines
    fs = MAX_FS
    while fs >= MIN_FS:
        lines = wrap(value, fs)
        if len(lines) * fs * LG + PAD_TOP + PAD_BOT <= h:
            break
        fs -= 0.5
    fs    = max(fs, MIN_FS)
    lines = wrap(value, fs)
    lh    = fs * LG
    n     = len(lines)
    sy    = PAD_BOT + (n - 1) * lh + fs
    usable_w = w - 4.0
    parts = ["q", f"0 0 {w:.2f} {h:.2f} re W n", "BT", f"/Helv {fs:.1f} Tf", "0 0 0 rg"]
    for i, line in enumerate(lines):
        yp = sy - i * lh
        if yp < PAD_BOT: break
        safe = line.replace("\\","\\\\").replace("(","\\(").replace(")","\\)")
        is_last = (i == len(lines) - 1) or (sy - (i+1)*lh < PAD_BOT)
        if not is_last:
            words = line.split(" ")
            gaps  = len(words) - 1
            tw    = min(max((usable_w - len(line)*fs*0.52) / gaps, 0), 4.0) if gaps > 0 else 0.0
        else:
            tw = 0.0
        parts.append(f"{tw:.3f} Tw")
        parts.append(f"2.0 {yp:.2f} Td" if i == 0 else f"0 {-lh:.2f} Td")
        parts.append(f"({safe}) Tj")
    parts += ["0 Tw", "ET", "Q"]
    return "\n".join(parts).encode()

def fill_pdf(template_bytes: bytes, fv: dict) -> bytes:
    reader = PdfReader(io.BytesIO(template_bytes))
    writer = PdfWriter()
    writer.append(reader)
    for page in writer.pages:
        writer.update_page_form_field_values(page, fv, auto_regenerate=False)
    if "/AcroForm" in writer._root_object:
        writer._root_object["/AcroForm"].update({
            NameObject("/NeedAppearances"): BooleanObject(False),
        })
    for page in writer.pages:
        for ref in page.get("/Annots", []):
            try:   annot = ref.get_object()
            except: continue
            if annot is None or not hasattr(annot, "get"):
                continue
            if annot.get("/Subtype") != "/Widget":
                continue
            fname    = str(annot.get("/T", ""))
            value    = fv.get(fname, "")
            rect     = tuple(float(v) for v in annot.get("/Rect", [0,0,0,0]))
            ap_bytes = None
            if fname in COMB_FIELDS:
                if value:
                    ap_bytes = _build_comb_ap(value, COMB_FIELDS[fname], rect)
            elif annot.get("/Ff") and bool(int(annot["/Ff"]) & (1 << 12)):
                if value:
                    ap_bytes = _build_multiline_ap(value, rect)
            if ap_bytes is None:
                continue
            stream = DecodedStreamObject()
            stream.set_data(ap_bytes)
            stream.update({
                NameObject("/Type"):    NameObject("/XObject"),
                NameObject("/Subtype"): NameObject("/Form"),
                NameObject("/BBox"):    RectangleObject([0, 0, rect[2]-rect[0], rect[3]-rect[1]]),
            })
            ap = DictionaryObject()
            ap[NameObject("/N")] = writer._add_object(stream)
            annot[NameObject("/AP")] = ap
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()

# ══════════════════════════════════════════════════════════════════════════════
# JOB LOG
# ══════════════════════════════════════════════════════════════════════════════

def load_jobs() -> list:
    if JOBS_FILE.exists():
        try:   return json.loads(JOBS_FILE.read_text())
        except: pass
    return []

def save_jobs(jobs: list):
    JOBS_FILE.write_text(json.dumps(jobs, indent=2, default=str))

def log_job(job: dict):
    jobs = load_jobs()
    jobs.insert(0, job)
    save_jobs(jobs[:500])

# ══════════════════════════════════════════════════════════════════════════════
# TWI DATASHEET GENERATOR  (ReportLab)
# ══════════════════════════════════════════════════════════════════════════════

DATASHEET_FIELDS = [
    # Event & Examination
    ("TWI Candidate Number",          "TWI Candidate Number",          "Event & Examination"),
    ("Course Name",                   "Course Name",                   "Event & Examination"),
    ("Batch Date",                    "Exam Date",                     "Event & Examination"),
    ("Venue",                         "Venue",                         "Event & Examination"),
    ("Application Type",              "Application Type",              "Event & Examination"),
    ("PCN or BGAS Approval Number",   "BGAS / PCN Cert No",            "Event & Examination"),
    ("CSWIP 3.1 Cert No",             "CSWIP 3.1 Cert No",             "Event & Examination"),
    ("Current CSWIP Qualifications",  "Current CSWIP Qualifications",  "Event & Examination"),
    # Candidate Details
    ("Candidate Name as per ID Proof","Candidate Name (as per ID)",    "Candidate Details"),
    ("Date of Birth",                 "Date of Birth",                 "Candidate Details"),
    ("Contact No",                    "Mobile / Contact No",           "Candidate Details"),
    ("WhatsApp Phone",                "WhatsApp Phone",                "Candidate Details"),
    ("Emergency Contact",             "Emergency Contact",             "Candidate Details"),
    ("Email",                         "Email",                         "Candidate Details"),
    ("Address",                       "Address Line 1",                "Candidate Details"),
    ("City",                          "City",                          "Candidate Details"),
    ("District",                      "District",                      "Candidate Details"),
    ("State",                         "State",                         "Candidate Details"),
    ("Pincode",                       "Pincode",                       "Candidate Details"),
    ("Current Designation",           "Current Designation",           "Candidate Details"),
    ("Current Job Started Year",      "Current Job Started Year",      "Candidate Details"),
    # Correspondence Address
    ("Correspondence Address 1",      "Correspondence Addr 1",         "Correspondence Address"),
    ("Correspondence Address 2",      "Correspondence Addr 2",         "Correspondence Address"),
    ("Correspondence Address 3",      "Correspondence Addr 3",         "Correspondence Address"),
    ("Correspondence Address 4",      "Correspondence Addr 4",         "Correspondence Address"),
    # Invoice Address
    ("Invoice Address 1",             "Invoice Addr 1",                "Invoice Address"),
    ("Invoice Address 2",             "Invoice Addr 2",                "Invoice Address"),
    ("Invoice Address 3",             "Invoice Addr 3",                "Invoice Address"),
    ("Invoice Address 4",             "Invoice Addr 4",                "Invoice Address"),
    # Sponsoring Company
    ("Sponsoring Address 1",          "Company Name / Addr 1",         "Sponsoring Company"),
    ("Sponsoring Address 2",          "Company Addr 2",                "Sponsoring Company"),
    ("Sponsoring Address 3",          "Company Addr 3",                "Sponsoring Company"),
    ("Sponsoring Pincode",            "Company Pincode",               "Sponsoring Company"),
    ("Approving Manager",             "Approving Manager",             "Sponsoring Company"),
    ("Company Order No",              "Company Order No",              "Sponsoring Company"),
    ("Contact Name",                  "Contact Name",                  "Sponsoring Company"),
    ("Contact Telephone",             "Contact Telephone",             "Sponsoring Company"),
    ("Contact Email",                 "Contact Email",                 "Sponsoring Company"),
    # Experience
    ("Total Years of Experience",     "Total Years of Experience",     "Experience"),
    ("Section 2 - Detailed Statement","Section 2 — Detailed Statement","Experience"),
    ("Section 5 - Detailed Statement","Section 5 — Detailed Statement","Experience"),
    # Verifier Details
    ("Verifier Name",                 "Verifier Name",                 "Verifier Details"),
    ("Verifier Company Name",         "Verifier Company",              "Verifier Details"),
    ("Verifier Designation",          "Verifier Designation",          "Verifier Details"),
    ("Verifier Professional Relation","Professional Relation",         "Verifier Details"),
    ("Verifier Phone",                "Verifier Phone",                "Verifier Details"),
    ("Verifier Email",                "Verifier Email",                "Verifier Details"),
    ("Verified Date",                 "Verified Date",                 "Verifier Details"),
    # Education
    ("SSLC Year",                     "SSLC Year",                     "Education"),
    ("Degree / Diploma Year",         "Degree / Diploma Year",         "Education"),
]

def generate_datasheet_pdf(record_data: dict, record_id: str = "") -> bytes:
    from reportlab.lib.styles import ParagraphStyle as PS
    from reportlab.lib.enums import TA_CENTER
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=15*mm, rightMargin=15*mm,
                            topMargin=12*mm,  bottomMargin=12*mm)
    W = A4[0] - 30*mm
    BLACK   = colors.black
    GREY_BG = colors.HexColor("#f5f5f5")
    sty_title = PS("t",   fontSize=13, fontName="Helvetica-Bold", alignment=TA_CENTER, spaceAfter=1*mm)
    sty_sub   = PS("s",   fontSize=9,  fontName="Helvetica",      alignment=TA_CENTER, spaceAfter=3*mm,
                   textColor=colors.HexColor("#555555"))
    sty_sec   = PS("sec", fontSize=9,  fontName="Helvetica-Bold")
    sty_label = PS("l",   fontSize=10, fontName="Helvetica-Bold")
    sty_value = PS("v",   fontSize=10, fontName="Helvetica")
    COL_LABEL = W * 0.38
    COL_VALUE = W * 0.62
    PAD = 5
    story = []
    cname = record_data.get("Candidate Name as per ID Proof", "") or record_id
    story.append(Paragraph("TWI Application — Data Verification Sheet", sty_title))
    story.append(Paragraph(
        f"Candidate: {cname}   |   Generated: {date.today().strftime('%d/%m/%Y')}   |   Record: {record_id}",
        sty_sub))
    sections = OrderedDict()
    for key, label, section in DATASHEET_FIELDS:
        sections.setdefault(section, []).append((key, label))
    table_data = []
    table_styles = [
        ("FONTNAME",      (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE",      (0,0), (-1,-1), 10),
        ("VALIGN",        (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",    (0,0), (-1,-1), PAD),
        ("BOTTOMPADDING", (0,0), (-1,-1), PAD),
        ("LEFTPADDING",   (0,0), (-1,-1), PAD+1),
        ("RIGHTPADDING",  (0,0), (-1,-1), PAD),
        ("LINEBELOW",     (0,0), (-1,-1), 0.3, colors.HexColor("#cccccc")),
        ("BOX",           (0,0), (-1,-1), 0.5, BLACK),
    ]
    row_idx = 0
    for sec_name, fields in sections.items():
        has_data = any(str(record_data.get(key, "") or "").strip() for key, _ in fields)
        if not has_data:
            continue
        table_data.append([Paragraph(sec_name.upper(), sty_sec), ""])
        table_styles.append(("SPAN",       (0, row_idx), (1, row_idx)))
        table_styles.append(("BACKGROUND", (0, row_idx), (1, row_idx), GREY_BG))
        table_styles.append(("FONTNAME",   (0, row_idx), (1, row_idx), "Helvetica-Bold"))
        row_idx += 1
        for key, label in fields:
            val = str(record_data.get(key, "") or "").strip()
            if not val:
                continue
            table_data.append([Paragraph(label, sty_label), Paragraph(val, sty_value)])
            row_idx += 1
    if table_data:
        tbl = Table(table_data, colWidths=[COL_LABEL, COL_VALUE])
        tbl.setStyle(TableStyle(table_styles))
        story.append(tbl)
    doc.build(story)
    buf.seek(0)
    return buf.read()

# ══════════════════════════════════════════════════════════════════════════════
# STUDENT DATA SHEET GENERATOR  (ReportLab — Blastline Institute branded)
# ══════════════════════════════════════════════════════════════════════════════

class _UnderlinedTitle(Flowable):
    """Title text with a precise underline drawn below the baseline."""
    def __init__(self, text, font="Helvetica-Bold", size=16):
        Flowable.__init__(self)
        self.text = text
        self.font = font
        self.size = size
        self.height = size * 1.8

    def wrap(self, avail_w, avail_h):
        self._w = avail_w
        return self._w, self.height

    def draw(self):
        c = self.canv
        c.setFont(self.font, self.size)
        c.setFillColor(colors.black)
        text_w = c.stringWidth(self.text, self.font, self.size)
        c.drawString(0, self.size * 0.35, self.text)
        c.setLineWidth(0.8)
        c.setStrokeColor(colors.black)
        c.line(0, self.size * 0.35 - 2, text_w, self.size * 0.35 - 2)

def _ss_style(extra=None):
    """Standard table style for student sheet."""
    base = [
        ("FONTNAME",      (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE",      (0,0), (-1,-1), 9),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING",    (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ("LEFTPADDING",   (0,0), (-1,-1), 5),
        ("RIGHTPADDING",  (0,0), (-1,-1), 4),
        ("GRID",          (0,0), (-1,-1), 0.5, colors.HexColor("#d0d0d0")),
        ("BOX",           (0,0), (-1,-1), 0.8, colors.black),
    ]
    if extra:
        base.extend(extra)
    return TableStyle(base)

def _fmt_date_ss(val: str) -> str:
    if not val:
        return ""
    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s[:11].strip(), fmt).strftime("%d-%b-%Y")
        except:
            continue
    return s

def _process_photo(photo_bytes: bytes, box_w_mm: float = 33, box_h_mm: float = 42) -> bytes:
    """
    Smart center-crop pipeline for candidate photos.

    Steps:
      1. Fix EXIF rotation  — phones embed orientation metadata; without
         this, portrait shots arrive sideways or upside-down.
      2. Convert to RGB     — removes alpha channel / palette modes that
         confuse JPEG output.
      3. Scale to fill      — resize so the shorter axis exactly fills the
         target box; the longer axis overflows (no letterboxing ever).
      4. Center crop        — trim the overflow symmetrically so the center
         of the image (where the face almost always is) is preserved.
      5. Return JPEG bytes  — compact, widely supported by ReportLab.

    Target ratio is derived from box_w_mm × box_h_mm (default 33×42 mm,
    a standard passport/ID portrait ratio of ~0.786 : 1).
    """
    from PIL import Image, ImageOps

    TARGET_W = box_w_mm
    TARGET_H = box_h_mm
    TARGET_RATIO = TARGET_W / TARGET_H          # ~0.786 for portrait ID

    img = Image.open(io.BytesIO(photo_bytes))

    # 1. Fix EXIF rotation
    img = ImageOps.exif_transpose(img)

    # 2. Ensure RGB
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    elif img.mode == "L":
        img = img.convert("RGB")

    src_w, src_h = img.size
    src_ratio    = src_w / src_h

    # 3. Scale to fill — the axis that would be short gets scaled up to fill
    if src_ratio > TARGET_RATIO:
        # Image is wider than target → constrain by height, width overflows
        new_h = 800                             # high-res intermediate height
        new_w = int(new_h * src_ratio)
    else:
        # Image is taller than target → constrain by width, height overflows
        new_w = int(800 * TARGET_RATIO)
        new_h = int(new_w / src_ratio)

    img = img.resize((new_w, new_h), Image.LANCZOS)

    # 4. Center crop to exact target canvas (800 × ~1018 for 33:42 ratio)
    crop_w = int(800 * TARGET_RATIO)
    crop_h = 800
    left   = (new_w - crop_w) // 2
    top    = (new_h - crop_h) // 2
    img    = img.crop((left, top, left + crop_w, top + crop_h))

    # 5. Output as JPEG bytes
    out = io.BytesIO()
    img.save(out, format="JPEG", quality=88, optimize=True)
    out.seek(0)
    return out.read()

def generate_student_sheet_pdf(d: dict, photo_bytes: bytes = None) -> bytes:
    """
    Generates a Blastline Institute branded Student Data Sheet PDF.
    d        — flat dict of CRM field values (keys as sent by Deluge)
    photo_bytes — raw image bytes of the candidate photo (optional)
    """
    PAGE_W, PAGE_H = A4
    LM = RM = 15 * mm
    TM = 12 * mm
    BM = 12 * mm
    CW = PAGE_W - LM - RM   # usable content width

    GREY_BG    = colors.HexColor("#f0f0f0")
    LIGHT_GREY = colors.HexColor("#d0d0d0")
    BLACK      = colors.black
    WHITE      = colors.white

    s_section  = ParagraphStyle("ss_sec",  fontName="Helvetica-Bold", fontSize=10,
                                spaceBefore=4*mm, spaceAfter=1*mm)
    s_label    = ParagraphStyle("ss_lbl",  fontName="Helvetica-Bold", fontSize=9)
    s_value    = ParagraphStyle("ss_val",  fontName="Helvetica",      fontSize=9)
    s_center   = ParagraphStyle("ss_ctr",  fontName="Helvetica-Bold", fontSize=9,
                                alignment=TA_CENTER)
    s_freetext = ParagraphStyle("ss_ft",   fontName="Helvetica",      fontSize=8.5, leading=13)
    s_office   = ParagraphStyle("ss_off",  fontName="Helvetica-Bold", fontSize=10,
                                alignment=TA_CENTER)

    def v(key, default=""):
        return str(d.get(key) or default).strip()

    def row(label, val):
        return [Paragraph(label, s_label), Paragraph(val, s_value)]

    buf   = io.BytesIO()
    doc   = SimpleDocTemplate(buf, pagesize=A4,
                              leftMargin=LM, rightMargin=RM,
                              topMargin=TM,  bottomMargin=BM)
    story = []

    # ── HEADER ────────────────────────────────────────────────────────────────
    if LOGO_PATH.exists():
        logo = RLImage(str(LOGO_PATH), width=52*mm, height=14*mm)
    else:
        logo = Paragraph("<b>BLASTLINE INSTITUTE</b>", s_label)

    hdr = Table([[_UnderlinedTitle("Student Data Sheet"), logo]],
                colWidths=[CW * 0.62, CW * 0.38])
    hdr.setStyle(TableStyle([
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
        ("ALIGN",        (1,0), (1,0),   "RIGHT"),
        ("TOPPADDING",   (0,0), (-1,-1), 0),
        ("BOTTOMPADDING",(0,0), (-1,-1), 0),
        ("LEFTPADDING",  (0,0), (-1,-1), 0),
        ("RIGHTPADDING", (0,0), (-1,-1), 0),
    ]))
    story.append(hdr)
    story.append(Spacer(1, 5*mm))
    story.append(HRFlowable(width="100%", thickness=1.5, color=BLACK, spaceAfter=3*mm))

    # ── CANDIDATE DETAILS ─────────────────────────────────────────────────────
    story.append(Paragraph("Candidate Details", s_section))

    addr_parts = [v("Address"), v("Address_line_2"), v("Address_line_3")]
    addr = ",\n".join(p for p in addr_parts if p)

    cand_rows = [
        row("Candidate Name",   v("Candidate_Name")),
        row("Date of Birth",    _fmt_date_ss(v("Date_of_Birth"))),
        row("Address",          addr),
        row("District",         v("District")),
        row("State",            v("State")),
        row("Pin-code",         v("Pincode")),
        row("Contact Number",   v("Mobile")),
        row("Contact Number 2", v("Home_Phone")),
        row("WhatsApp Number",  v("Asst_Phone")),
        row("Emergency Number", v("Emergency_Phone")),
        row("Email Id",         v("Email")),
    ]
    cand_tbl = Table(cand_rows, colWidths=[42*mm, 72*mm])
    cand_tbl.setStyle(_ss_style([
        ("FONTNAME", (0,0), (0,-1), "Helvetica-Bold"),
        ("VALIGN",   (0,2), (1,2),  "TOP"),
    ]))

    # Photo — auto center-crop to portrait ID ratio before embedding
    if photo_bytes:
        try:
            processed  = _process_photo(photo_bytes, box_w_mm=33, box_h_mm=42)
            photo_cell = RLImage(io.BytesIO(processed), width=33*mm, height=42*mm)
        except Exception as photo_err:
            print(f"[PHOTO] processing failed: {photo_err}")
            photo_cell = Paragraph("Photo", s_center)
    else:
        photo_cell = Paragraph("Photo", s_center)

    photo_box = Table([[photo_cell]], colWidths=[35*mm], rowHeights=[44*mm])
    photo_box.setStyle(TableStyle([
        ("BOX",           (0,0), (-1,-1), 0.8, BLACK),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ("ALIGN",         (0,0), (-1,-1), "CENTER"),
        ("TOPPADDING",    (0,0), (-1,-1), 2),
        ("BOTTOMPADDING", (0,0), (-1,-1), 2),
    ]))

    today_str  = date.today().strftime("%d-%b-%Y")
    student_id = v("Student_ID_No")

    right_data = [
        [Paragraph(f"<b>Date:</b>  {today_str}", s_value)],
        [Spacer(1, 2*mm)],
        [Paragraph(f"<b>Student ID:</b>  {student_id}", s_value)],
        [Spacer(1, 3*mm)],
        [photo_box],
    ]
    right_tbl = Table(right_data, colWidths=[45*mm])
    right_tbl.setStyle(TableStyle([
        ("TOPPADDING",    (0,0), (-1,-1), 2),
        ("BOTTOMPADDING", (0,0), (-1,-1), 2),
        ("LEFTPADDING",   (0,0), (-1,-1), 4),
        ("RIGHTPADDING",  (0,0), (-1,-1), 0),
    ]))

    outer = Table([[cand_tbl, right_tbl]], colWidths=[CW*0.745, CW*0.255])
    outer.setStyle(TableStyle([
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",   (0,0), (-1,-1), 0),
        ("BOTTOMPADDING",(0,0), (-1,-1), 0),
        ("LEFTPADDING",  (0,0), (-1,-1), 0),
        ("RIGHTPADDING", (0,0), (-1,-1), 0),
    ]))
    story.append(outer)
    story.append(Spacer(1, 4*mm))

    # ── COURSE + EXAM  (side by side) ─────────────────────────────────────────
    GAP    = 4*mm
    HALF_W = (CW - GAP) / 2

    course_data = [
        [Paragraph("<b>Course Details</b>", s_label), ""],
        row("Course Name",  v("Course_Name")),
        row("Course Date",  _fmt_date_ss(v("Course_Date"))),
        row("Fee",          v("Course_Fees")),
    ]
    course_tbl = Table(course_data, colWidths=[HALF_W*0.42, HALF_W*0.58])
    course_tbl.setStyle(_ss_style([
        ("SPAN",       (0,0), (1,0)),
        ("BACKGROUND", (0,0), (1,0), GREY_BG),
        ("FONTNAME",   (0,0), (1,0), "Helvetica-Bold"),
        ("FONTNAME",   (0,1), (0,-1), "Helvetica-Bold"),
    ]))

    exam_data = [
        [Paragraph("<b>Seminar &amp; Exam Details</b>", s_label), ""],
        row("Exam Date", _fmt_date_ss(v("Exam_Date"))),
        row("Fee",       v("Selected_Course_Fees")),
        ["", ""],
    ]
    exam_tbl = Table(exam_data, colWidths=[HALF_W*0.42, HALF_W*0.58])
    exam_tbl.setStyle(_ss_style([
        ("SPAN",       (0,0), (1,0)),
        ("BACKGROUND", (0,0), (1,0), GREY_BG),
        ("FONTNAME",   (0,0), (1,0), "Helvetica-Bold"),
        ("FONTNAME",   (0,1), (0,-1), "Helvetica-Bold"),
    ]))

    sbs = Table([[course_tbl, Spacer(GAP, 1), exam_tbl]], colWidths=[HALF_W, GAP, HALF_W])
    sbs.setStyle(TableStyle([
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",   (0,0), (-1,-1), 0),
        ("BOTTOMPADDING",(0,0), (-1,-1), 0),
        ("LEFTPADDING",  (0,0), (-1,-1), 0),
        ("RIGHTPADDING", (0,0), (-1,-1), 0),
    ]))
    story.append(sbs)
    story.append(Spacer(1, 5*mm))

    # ── FREE TEXT QUESTIONS ───────────────────────────────────────────────────
    fq_data = [
        [Paragraph("Have you done any other course earlier in Blastline? (Please specify course &amp; batch)", s_freetext),
         Paragraph(v("Previous_Course"), s_value)],
        [Paragraph("Contact Person Name (Whom did you contact in Blastline)?", s_freetext),
         Paragraph(v("Referrer"), s_value)],
        [Paragraph("How did you know about Blastline Institute?", s_freetext),
         Paragraph(v("Lead_Source"), s_value)],
    ]
    fq_tbl = Table(fq_data, colWidths=[CW*0.52, CW*0.48])
    fq_tbl.setStyle(_ss_style([
        ("FONTNAME",      (0,0), (-1,-1), "Helvetica"),
        ("TOPPADDING",    (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("VALIGN",        (0,0), (-1,-1), "TOP"),
    ]))
    story.append(fq_tbl)
    story.append(Spacer(1, 5*mm))
    story.append(HRFlowable(width="100%", thickness=0.8,
                            color=colors.HexColor("#d0d0d0"), spaceAfter=4*mm))

    # ── IF WORKING ────────────────────────────────────────────────────────────
    story.append(Paragraph("If Working,", s_section))
    C1 = CW * 0.28; C2 = CW * 0.22
    work_data = [
        [Paragraph("Name of the Company",  s_label), Paragraph(v("Name_of_the_Company"), s_value),
         Paragraph("Email Id",             s_label), Paragraph(v("HR_Email_Id"),          s_value)],
        [Paragraph("Name of HR Manager",   s_label), Paragraph(v("Name_of_HR_Manager"),   s_value),
         Paragraph("Contact No.",          s_label), Paragraph(v("HR_Phone_Number"),       s_value)],
    ]
    work_tbl = Table(work_data, colWidths=[C1, C2, C1, C2])
    work_tbl.setStyle(_ss_style([
        ("FONTNAME", (0,0), (0,-1), "Helvetica-Bold"),
        ("FONTNAME", (2,0), (2,-1), "Helvetica-Bold"),
    ]))
    story.append(work_tbl)
    story.append(Spacer(1, 6*mm))

    # ── FOR OFFICE USE ────────────────────────────────────────────────────────
    story.append(HRFlowable(width="100%", thickness=1, color=BLACK, spaceAfter=3*mm))
    story.append(Paragraph("<b>For Office Use</b>", s_office))
    story.append(Spacer(1, 2*mm))

    R1 = CW*0.38; R2 = CW*0.12; R3 = CW*0.28; R4 = CW*0.22
    remarks_data = [
        [Paragraph("Remarks:", s_label), "",
         Paragraph("Verified By - Name:", s_label), ""],
        ["", "", Paragraph("Signature:", s_label), ""],
    ]
    remarks_tbl = Table(remarks_data, colWidths=[R1, R2, R3, R4],
                        rowHeights=[10*mm, 10*mm])
    remarks_tbl.setStyle(_ss_style([
        ("SPAN",    (0,0), (0,1)),
        ("SPAN",    (1,0), (1,1)),
        ("VALIGN",  (0,0), (-1,-1), "TOP"),
        ("FONTNAME",(0,0), (0,-1), "Helvetica-Bold"),
        ("FONTNAME",(2,0), (2,-1), "Helvetica-Bold"),
    ]))
    story.append(remarks_tbl)

    doc.build(story)
    buf.seek(0)
    return buf.read()

# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/generate")
async def generate(request: Request):
    body        = await request.json()
    record_data = body.get("record_data", {})
    record_id   = body.get("record_id", "unknown")
    job_id      = str(uuid.uuid4())
    if not record_data:
        raise HTTPException(400, "record_data is required")
    if not PDF_TEMPLATE_PATH.exists():
        raise HTTPException(500, "PDF template not found on server")
    cname = ""
    for k in ("Candidate Name as per ID Proof", "Full_Name", "Last_Name", "Name", "name"):
        if record_data.get(k):
            cname = str(record_data[k]).strip().replace(" ", "_"); break
    cname    = cname or "Candidate"
    filename = body.get("filename") or f"TWI_{cname}_{datetime.now().strftime('%Y%m%d')}.pdf"
    try:
        template  = PDF_TEMPLATE_PATH.read_bytes()
        mapped    = apply_mappings(record_data)
        fv        = build_field_values(mapped)
        pdf_bytes = fill_pdf(template, fv)
        if record_id != "unknown":
            for old_job in load_jobs():
                if old_job.get("record_id") == record_id and old_job.get("type","twi") == "twi":
                    old_pdf = PDF_STORE / f"{old_job['id']}.pdf"
                    if old_pdf.exists(): old_pdf.unlink()
        (PDF_STORE / f"{job_id}.pdf").write_bytes(pdf_bytes)
        log_job({"id": job_id, "record_id": record_id, "candidate": cname.replace("_"," "),
                 "filename": filename, "status": "Done", "type": "twi",
                 "created_at": datetime.now().isoformat(), "error": None})
        return JSONResponse({"ok": True, "job_id": job_id, "filename": filename})
    except Exception as ex:
        log_job({"id": job_id, "record_id": record_id, "candidate": cname.replace("_"," "),
                 "filename": filename, "status": "Error", "type": "twi",
                 "created_at": datetime.now().isoformat(), "error": str(ex)})
        raise HTTPException(500, str(ex))


@app.post("/datasheet")
async def datasheet(request: Request):
    body        = await request.json()
    record_data = body.get("record_data", {})
    record_id   = body.get("record_id", "unknown")
    job_id      = str(uuid.uuid4())
    if not record_data:
        raise HTTPException(400, "record_data is required")
    cname = ""
    for k in ("Candidate Name as per ID Proof", "Name", "name"):
        if record_data.get(k):
            cname = str(record_data[k]).strip().replace(" ", "_"); break
    cname    = cname or "Candidate"
    filename = f"DataSheet_{cname}_{datetime.now().strftime('%Y%m%d')}.pdf"
    try:
        pdf_bytes = generate_datasheet_pdf(record_data, record_id)
        if record_id != "unknown":
            for old_job in load_jobs():
                if old_job.get("record_id") == record_id and old_job.get("type") == "datasheet":
                    old_pdf = PDF_STORE / f"{old_job['id']}.pdf"
                    if old_pdf.exists(): old_pdf.unlink()
        (PDF_STORE / f"{job_id}.pdf").write_bytes(pdf_bytes)
        log_job({"id": job_id, "record_id": record_id, "candidate": cname.replace("_"," "),
                 "filename": filename, "status": "Done", "type": "datasheet",
                 "created_at": datetime.now().isoformat(), "error": None})
        return JSONResponse({"ok": True, "job_id": job_id, "filename": filename})
    except Exception as ex:
        log_job({"id": job_id, "record_id": record_id, "candidate": cname.replace("_"," "),
                 "filename": filename, "status": "Error", "type": "datasheet",
                 "created_at": datetime.now().isoformat(), "error": str(ex)})
        raise HTTPException(500, str(ex))


@app.post("/studentsheet")
async def studentsheet(request: Request):
    """
    Generate Blastline Institute Student Data Sheet.
    Payload:
        record_data  — flat dict of CRM field values
        record_id    — CRM record ID (used for deduplication)
        photo_b64    — optional base64-encoded candidate photo bytes
    """
    body        = await request.json()
    record_data = body.get("record_data", {})
    record_id   = str(body.get("record_id", "unknown"))
    photo_b64   = body.get("photo_b64", "")
    job_id      = str(uuid.uuid4())

    if not record_data:
        raise HTTPException(400, "record_data is required")

    cname = ""
    for k in ("Candidate_Name", "First_Name", "Name", "name"):
        if record_data.get(k):
            cname = str(record_data[k]).strip().replace(" ", "_"); break
    cname    = cname or "Student"
    filename = f"StudentSheet_{cname}_{datetime.now().strftime('%Y%m%d')}.pdf"

    # Decode photo if provided
    photo_bytes = None
    if photo_b64:
        try:
            photo_bytes = base64.b64decode(photo_b64)
        except Exception:
            photo_bytes = None

    try:
        pdf_bytes = generate_student_sheet_pdf(record_data, photo_bytes=photo_bytes)

        # Remove previous student sheet for same record
        if record_id != "unknown":
            for old_job in load_jobs():
                if old_job.get("record_id") == record_id and old_job.get("type") == "studentsheet":
                    old_pdf = PDF_STORE / f"{old_job['id']}.pdf"
                    if old_pdf.exists(): old_pdf.unlink()

        (PDF_STORE / f"{job_id}.pdf").write_bytes(pdf_bytes)
        log_job({"id": job_id, "record_id": record_id, "candidate": cname.replace("_"," "),
                 "filename": filename, "status": "Done", "type": "studentsheet",
                 "created_at": datetime.now().isoformat(), "error": None})
        return JSONResponse({"ok": True, "job_id": job_id, "filename": filename})

    except Exception as ex:
        log_job({"id": job_id, "record_id": record_id, "candidate": cname.replace("_"," "),
                 "filename": filename, "status": "Error", "type": "studentsheet",
                 "created_at": datetime.now().isoformat(), "error": str(ex)})
        raise HTTPException(500, str(ex))


@app.get("/pdf/{job_id}", response_class=Response)
async def download_pdf(job_id: str):
    pdf_path = PDF_STORE / f"{job_id}.pdf"
    if not pdf_path.exists():
        raise HTTPException(404, detail="PDF not found or expired. Please regenerate from CRM.")
    age_days = (datetime.now().timestamp() - pdf_path.stat().st_mtime) / 86400
    if age_days > 7:
        pdf_path.unlink()
        raise HTTPException(410, detail="PDF link expired (7 days). Please regenerate from CRM.")
    jobs     = load_jobs()
    job      = next((j for j in jobs if j["id"] == job_id), {})
    filename = job.get("filename", "Document.pdf")
    return Response(
        content=pdf_path.read_bytes(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"',
                 "Cache-Control": "no-cache"},
    )


@app.get("/fields")
async def list_fields():
    fields = get_pdf_fields()
    return JSONResponse({
        "total":            len(fields),
        "text_fields":      sorted([k for k,v in fields.items() if v["type"] == "/Tx"]),
        "button_fields":    sorted([k for k,v in fields.items() if v["type"] == "/Btn"]),
        "comb_fields":      sorted([k for k,v in fields.items() if v["comb"]]),
        "multiline_fields": sorted([k for k,v in fields.items() if v["multiline"]]),
        "all":              {k: v for k, v in sorted(fields.items())},
    })


@app.get("/health")
async def health():
    return JSONResponse({
        "ok":            PDF_TEMPLATE_PATH.exists(),
        "template":      str(PDF_TEMPLATE_PATH) if PDF_TEMPLATE_PATH.exists() else "MISSING",
        "logo":          str(LOGO_PATH) if LOGO_PATH.exists() else "MISSING",
        "fields":        len(get_pdf_fields()),
        "jobs":          len(load_jobs()),
    })


@app.get("/queue", response_class=HTMLResponse)
async def queue_page(auth: str = Depends(require_auth)):
    jobs = load_jobs()
    rows = ""
    TYPE_COLORS = {"twi": "#6366f1", "datasheet": "#0ea5e9", "studentsheet": "#10b981"}
    for job in jobs:
        status = job.get("status", "?")
        jtype  = job.get("type", "twi")
        color  = {"Done":"#10b981","Error":"#ef4444"}.get(status,"#f59e0b")
        badge  = (f'<span style="background:{color};color:#fff;padding:2px 10px;'
                  f'border-radius:12px;font-size:11px;font-weight:600">{status}</span>')
        tbadge = (f'<span style="background:{TYPE_COLORS.get(jtype,"#6366f1")};color:#fff;'
                  f'padding:1px 8px;border-radius:8px;font-size:10px">{jtype}</span>')
        ts     = job.get("created_at","")[:16].replace("T"," ")
        dl     = (f'<a href="/pdf/{job["id"]}" target="_blank" style="background:#3b82f6;'
                  f'color:#fff;padding:4px 14px;border-radius:5px;text-decoration:none;'
                  f'font-size:12px">⬇ Download</a>'
                  if status == "Done" else
                  f'<span style="color:#ef4444;font-size:11px">{job.get("error","")[:60]}</span>')
        rows  += (f'<tr><td>{job.get("candidate","—")}</td>'
                  f'<td style="color:#64748b;font-size:11px">{job.get("record_id","")}</td>'
                  f'<td>{tbadge}</td>'
                  f'<td>{job.get("filename","—")}</td>'
                  f'<td>{badge}</td>'
                  f'<td style="color:#64748b;font-size:12px">{ts}</td>'
                  f'<td>{dl}</td></tr>')

    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TWI PDF Queue</title>
<meta http-equiv="refresh" content="20">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;
     background:#f8fafc;color:#1e293b;padding:24px}}
h1{{font-size:20px;font-weight:700;margin-bottom:4px}}
.sub{{font-size:12px;color:#64748b;margin-bottom:20px}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;
       overflow:hidden;box-shadow:0 1px 6px rgba(0,0,0,.08)}}
th{{background:#f1f5f9;font-size:11px;font-weight:700;text-transform:uppercase;
    letter-spacing:.05em;color:#64748b;padding:10px 14px;text-align:left;
    border-bottom:1px solid #e2e8f0}}
td{{padding:10px 14px;font-size:13px;border-bottom:1px solid #f1f5f9;vertical-align:middle}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#f8fafc}}
.empty{{text-align:center;padding:48px;color:#94a3b8;font-size:14px}}
.refresh{{float:right;font-size:12px;color:#3b82f6;text-decoration:none;
          padding:6px 14px;border:1px solid #3b82f6;border-radius:5px}}
</style></head><body>
<h1>📋 TWI PDF Queue</h1>
<p class="sub">Auto-refreshes every 20 seconds &nbsp;·&nbsp; {len(jobs)} jobs
<a class="refresh" href="/queue">↻ Refresh</a></p>
<table><thead><tr>
<th>Candidate</th><th>CRM Record</th><th>Type</th><th>Filename</th>
<th>Status</th><th>Time</th><th>Action</th>
</tr></thead><tbody>
{rows if jobs else '<tr><td colspan="7" class="empty">No jobs yet.</td></tr>'}
</tbody></table></body></html>""")


@app.get("/", response_class=HTMLResponse)
async def root(auth: str = Depends(require_auth)):
    tmpl_ok    = PDF_TEMPLATE_PATH.exists()
    logo_ok    = LOGO_PATH.exists()
    fields     = len(get_pdf_fields())
    jobs       = len(load_jobs())
    return HTMLResponse(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>TWI PDF Engine</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;
     background:#f8fafc;color:#1e293b;padding:40px;max-width:640px;margin:0 auto}}
h1{{font-size:22px;font-weight:700;margin-bottom:6px}}
.sub{{color:#64748b;font-size:13px;margin-bottom:32px}}
.card{{background:#fff;border-radius:10px;padding:20px 24px;
       box-shadow:0 1px 6px rgba(0,0,0,.08);margin-bottom:16px}}
.card h2{{font-size:13px;font-weight:700;text-transform:uppercase;
          letter-spacing:.05em;color:#64748b;margin-bottom:12px}}
.row{{display:flex;justify-content:space-between;align-items:center;
      padding:7px 0;border-bottom:1px solid #f1f5f9;font-size:13px}}
.row:last-child{{border-bottom:none}}
.val{{font-weight:600}}
.ok{{color:#10b981}}.err{{color:#ef4444}}
a.btn{{display:inline-block;margin:4px 4px 0 0;padding:7px 16px;border-radius:6px;
       text-decoration:none;font-size:13px;font-weight:500;background:#3b82f6;color:#fff}}
a.btn.grey{{background:#e2e8f0;color:#475569}}
</style></head><body>
<h1>📋 TWI PDF Rendering Engine</h1>
<p class="sub">Blastline Institute — PDF Generation Service</p>
<div class="card"><h2>Status</h2>
  <div class="row"><span>TWI PDF Template</span>
    <span class="val {'ok' if tmpl_ok else 'err'}">{'Found (' + str(fields) + ' fields)' if tmpl_ok else 'MISSING'}</span></div>
  <div class="row"><span>Institute Logo</span>
    <span class="val {'ok' if logo_ok else 'err'}">{'Found' if logo_ok else 'MISSING'}</span></div>
  <div class="row"><span>Jobs processed</span><span class="val">{jobs}</span></div>
  <div class="row"><span>Server</span><span class="val ok">Running ✓</span></div>
</div>
<div class="card"><h2>Endpoints</h2>
  <a class="btn" href="/queue">📋 Queue</a>
  <a class="btn" href="/mappings">⚙️ Mappings</a>
  <a class="btn grey" href="/fields">Fields JSON</a>
  <a class="btn grey" href="/health">Health</a>
  <a class="btn grey" href="/docs">API Docs</a>
</div>
<div class="card"><h2>PDF Types</h2>
  <div class="row"><span>POST /generate</span><span style="color:#6366f1;font-weight:600">TWI Enrolment Form</span></div>
  <div class="row"><span>POST /datasheet</span><span style="color:#0ea5e9;font-weight:600">TWI Data Verification Sheet</span></div>
  <div class="row"><span>POST /studentsheet</span><span style="color:#10b981;font-weight:600">Student Data Sheet</span></div>
</div>
</body></html>""")


@app.get("/mappings", response_class=HTMLResponse)
async def mappings_page(auth: str = Depends(require_auth)):
    pdf_fields  = get_pdf_fields()
    pdf_options = sorted([k for k,v in pdf_fields.items() if v["type"] == "/Tx"])
    manual      = load_manual_mappings()

    KNOWN_DELUGE_KEYS = [
        "Candidate Name as per ID Proof", "TWI Candidate Number", "Date of Birth",
        "Application Type",
        "Email", "Contact No", "Emergency Contact", "WhatsApp Phone",
        "Address", "City", "District", "State", "Pincode",
        "Current Designation", "Current Job Started Year",
        "Correspondence Address 1", "Correspondence Address 2",
        "Correspondence Address 3", "Correspondence Address 4",
        "Invoice Address 1", "Invoice Address 2", "Invoice Address 3", "Invoice Address 4",
        "Course Name", "Batch Date", "Venue",
        "PCN or BGAS Approval Number", "CSWIP 3.1 Cert No", "Current CSWIP Qualifications",
        "Sponsoring Address 1", "Sponsoring Address 2", "Sponsoring Address 3",
        "Sponsoring Pincode", "Approving Manager", "Company Order No",
        "Contact Name", "Contact Telephone", "Contact Email",
        "Total Years of Experience",
        "Section 2 - Detailed Statement", "Section 5 - Detailed Statement",
        "Verifier Name", "Verifier Phone", "Verifier Email",
        "Verifier Company Name", "Verifier Designation",
        "Verifier Professional Relation", "Verified Date",
        "SSLC Year", "Degree / Diploma Year",
    ]

    def auto_match(zk):
        for pattern, target in AUTO_MAP_RULES:
            if re.search(pattern, zk, re.IGNORECASE):
                return target
        pdf_text = {k for k,v in pdf_fields.items() if v["type"] == "/Tx"}
        norm_zk  = re.sub(r"[^a-z0-9]", "", zk.lower())
        for pf in pdf_text:
            norm_pf = re.sub(r"[^a-z0-9]", "", pf.lower())
            if norm_zk and norm_pf and len(norm_pf) >= 3 and (norm_zk in norm_pf or norm_pf in norm_zk):
                return f"{pf} (fuzzy)"
        return "__ignore__"

    rows = ""
    for zk in KNOWN_DELUGE_KEYS:
        auto        = auto_match(zk)
        manual_val  = manual.get(zk, "")
        effective   = manual_val if manual_val else auto
        is_override = bool(manual_val)
        is_ignore   = (effective == "__ignore__")

        def make_opts(selected):
            o  = f'<option value=""{"  selected" if not selected else ""}>(ignore / skip)</option>'
            o += f'<option value="__dob__"{"  selected" if selected == "__dob__" else ""}>__dob__ (Date of Birth comb fields)</option>'
            for f in pdf_options:
                sel = " selected" if f == selected else ""
                o  += f'<option value="{f}"{sel}>{f}</option>'
            return o

        tag_color  = "#f59e0b" if is_override else ("#6b7280" if is_ignore else "#10b981")
        tag_label  = "manual" if is_override else ("ignored" if is_ignore else "auto")
        fuzzy_warn = " ⚠" if "fuzzy" in auto and not is_override else ""

        rows += f"""
        <tr>
          <td style="font-weight:500">{zk}</td>
          <td style="color:#64748b;font-size:12px">{auto}{fuzzy_warn}</td>
          <td>
            <select name="{zk}" style="width:100%;padding:4px 6px;border:1px solid #e2e8f0;
              border-radius:4px;font-size:12px;background:#fff">
              {make_opts(manual_val or effective)}
            </select>
          </td>
          <td style="text-align:center">
            <span style="background:{tag_color};color:#fff;padding:2px 8px;
              border-radius:10px;font-size:10px;font-weight:600">{tag_label}</span>
          </td>
        </tr>"""

    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Field Mappings — TWI PDF Engine</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;
     background:#f8fafc;color:#1e293b;padding:24px}}
h1{{font-size:18px;font-weight:700;margin-bottom:4px}}
.sub{{font-size:12px;color:#64748b;margin-bottom:20px}}
.legend{{display:flex;gap:16px;margin-bottom:16px;font-size:12px}}
.dot{{width:10px;height:10px;border-radius:50%;display:inline-block;margin-right:4px}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;
       overflow:hidden;box-shadow:0 1px 6px rgba(0,0,0,.08)}}
th{{background:#f1f5f9;font-size:11px;font-weight:700;text-transform:uppercase;
    letter-spacing:.05em;color:#64748b;padding:10px 14px;text-align:left;
    border-bottom:1px solid #e2e8f0}}
td{{padding:8px 14px;font-size:13px;border-bottom:1px solid #f1f5f9;vertical-align:middle}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#f8fafc}}
.btn{{display:inline-block;padding:8px 20px;border-radius:6px;border:none;cursor:pointer;
      font-size:13px;font-weight:600;background:#3b82f6;color:#fff;margin-right:8px}}
.btn.grey{{background:#e2e8f0;color:#475569;text-decoration:none;display:inline-block}}
.btn.red{{background:#ef4444}}
.actions{{margin-bottom:20px;display:flex;align-items:center;gap:8px}}
</style></head><body>
<h1>⚙️ Field Mappings</h1>
<p class="sub">
  Map each CRM field (sent by Deluge) to the corresponding PDF form field.<br>
  <b>Manual overrides</b> take priority over automatic regex rules.
</p>
<div class="legend">
  <span><span class="dot" style="background:#10b981"></span>auto</span>
  <span><span class="dot" style="background:#f59e0b"></span>manual override</span>
  <span><span class="dot" style="background:#6b7280"></span>ignored / datasheet only</span>
  <span style="color:#f59e0b">⚠ fuzzy match</span>
</div>
<div class="actions">
  <a href="/" class="btn grey">← Home</a>
  <button type="submit" form="mapform" class="btn">💾 Save Mappings</button>
  <button type="submit" form="resetform" class="btn red">↺ Reset All to Auto</button>
</div>
<form id="mapform" method="POST" action="/mappings">
<table><thead><tr>
  <th style="width:28%">CRM Field (Deluge key)</th>
  <th style="width:28%">Auto-matched PDF field</th>
  <th style="width:36%">Override → PDF field</th>
  <th style="width:8%">Status</th>
</tr></thead><tbody>{rows}</tbody></table>
<div style="padding:16px 0"><button type="submit" class="btn">💾 Save Mappings</button></div>
</form>
<form id="resetform" method="POST" action="/mappings/reset"></form>
</body></html>""")


@app.post("/mappings")
async def save_mappings(request: Request, auth: str = Depends(require_auth)):
    form    = await request.form()
    mf      = DATA_DIR / "manual_mappings.json"
    updated = {}
    for k, v in form.items():
        v = v.strip()
        if v:
            updated[k] = v
    mf.write_text(json.dumps(updated, indent=2))
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/mappings", status_code=303)


@app.post("/mappings/reset")
async def reset_mappings(auth: str = Depends(require_auth)):
    mf = DATA_DIR / "manual_mappings.json"
    if mf.exists():
        mf.write_text("{}")
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/mappings", status_code=303)


@app.get("/debug")
async def debug():
    import sys
    try:   app_files  = [f.name for f in _APP_DIR.iterdir()]
    except Exception as e: app_files = [str(e)]
    try:   data_files = [f.name for f in DATA_DIR.iterdir()]
    except Exception as e: data_files = [str(e)]
    return JSONResponse({
        "cwd":             os.getcwd(),
        "app_dir":         str(_APP_DIR),
        "data_dir":        str(DATA_DIR),
        "template_path":   str(PDF_TEMPLATE_PATH),
        "template_exists": PDF_TEMPLATE_PATH.exists(),
        "logo_path":       str(LOGO_PATH),
        "logo_exists":     LOGO_PATH.exists(),
        "app_dir_files":   sorted(app_files),
        "data_dir_files":  sorted(data_files),
        "python":          sys.version,
        "env_DATA_DIR":    os.environ.get("DATA_DIR", "not set"),
    })


@app.post("/upload-template")
async def upload_template(request: Request):
    pdf_bytes = await request.body()
    if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
        raise HTTPException(400, "Invalid PDF — body must be raw PDF bytes")
    PDF_TEMPLATE_PATH.write_bytes(pdf_bytes)
    global _pdf_fields_cache
    _pdf_fields_cache = {}
    fields = len(get_pdf_fields())
    return JSONResponse({
        "ok":      True,
        "path":    str(PDF_TEMPLATE_PATH),
        "size_kb": len(pdf_bytes) // 1024,
        "fields":  fields,
    })
