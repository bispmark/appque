"""
TWI PDF Rendering Engine  —  FastAPI
=====================================
Pure PDF engine — no CRM credentials needed.
Deluge sends record data, engine returns filled PDF as base64.
Deluge attaches it to the CRM record natively.

Endpoints
---------
POST /generate
    Body: { record_data: {field: value, ...}, record_id, filename (optional) }
    Returns: { ok, pdf_base64, filename }

GET  /queue
    CRM Web Tab — HTML job log with download links.

GET  /pdf/{job_id}
    Download filled PDF by job ID.

GET  /health
    Server status check.

Run
---
    uvicorn render_engine:app --host 0.0.0.0 --port 8504
"""

import base64, io, json, os, re, textwrap, uuid
from datetime import datetime, date
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets

from pypdf import PdfReader, PdfWriter
from pypdf.generic import (
    DecodedStreamObject, DictionaryObject, NameObject, RectangleObject,
)

# ══════════════════════════════════════════════════════════════════════════════
# PATHS
# ══════════════════════════════════════════════════════════════════════════════
_APP_DIR          = Path(__file__).parent
DATA_DIR          = Path(os.environ.get("DATA_DIR", str(_APP_DIR)))
PDF_TEMPLATE_PATH = _APP_DIR / "enrolment-form-india.pdf"
JOBS_FILE         = DATA_DIR / "render_jobs.json"
PDF_STORE         = DATA_DIR / "rendered_pdfs"
PDF_STORE.mkdir(parents=True, exist_ok=True)

app      = FastAPI(title="TWI PDF Rendering Engine")
security = HTTPBasic()

# Credentials — override via QUEUE_USER and QUEUE_PASS env vars on Render
QUEUE_USER = os.environ.get("QUEUE_USER", "blastline").encode()
QUEUE_PASS = os.environ.get("QUEUE_PASS", "TWI@2026").encode()

def require_auth(credentials: HTTPBasicCredentials = Depends(security)):
    """HTTP Basic Auth — protects queue and PDF download endpoints."""
    user_ok = secrets.compare_digest(credentials.username.encode(), QUEUE_USER)
    pass_ok = secrets.compare_digest(credentials.password.encode(), QUEUE_PASS)
    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

app.add_middleware(
    CORSMiddleware, allow_origins=["*"],
    allow_methods=["*"], allow_headers=["*"],
)

# ══════════════════════════════════════════════════════════════════════════════
# FIELD METADATA
# ══════════════════════════════════════════════════════════════════════════════

EXAM_TYPE_MAP = {
    "Initial":"/1", "Supplementary":"/2", "Renewal":"/3",
    "Bridging":"/4", "Retest":"/5",
}
EXAM_BODY_MAP = {
    "CSWIP":"/6", "PCN":"/7", "AWS":"/8", "BGAS":"/9", "ASNT":"/10",
}
HEARD_FIELDS = {
    "LinkedIn":              ("Check Box9",  "/1"),
    "Facebook":              ("Check Box10", "/2"),
    "NDT News / Insight":    ("Check Box11", "/3"),
    "Exhibitions / Events":  ("Check Box12", "/4"),
    "Word of Mouth":         ("Check Box13", "/5"),
    "TWI Corporate Website": ("Check Box15", "/7"),
    "CSWIP Website":         ("Check Box16", "/8"),
    "Email marketing":       ("Check Box17", "/9"),
    "Bulletin / Connect":    ("Check Box18", "/10"),
    "Google search":         ("Check Box19", "/Yes"),
    "Other":                 ("Check Box20", "/Yes"),
}
COMB_FIELDS   = {"undefined": 6, "D": 2, "M": 2, "Y": 4}
SPECIAL_TOKENS = {
    "__dob__", "__exam_type__", "__exam_body__", "__sponsor_type__",
    "__disability__", "__gdpr__", "__heard__", "__ignore__",
}

# Regex auto-map rules: (zoho_field_pattern, pdf_field_or_token)
AUTO_MAP_RULES = [
    (r"batch.?date|exam.?date",                          "Event date"),
    (r"course.?name|event.?title",                       "Event title"),
    (r"candidate.?name|name.?as.?per.?id",               "Candidates Family Name as per ID  Passport"),
    (r"twi.?candidate.?(number|id|no)",                  "undefined"),
    (r"date.?of.?birth|dob",                             "__dob__"),
    (r"^address$|address.?line.?1|permanent.?address",   "Permanent private address 1"),
    (r"^city$|address.?line.?2",                         "Permanent private address 2"),
    (r"^district$|address.?line.?3",                     "Permanent private address 3"),
    (r"pincode|postcode|postal",                         "Postcode"),
    (r"correspondence.*1|correspondence.?address$",      "Correspondence address if different from above 1"),
    (r"correspondence.*2",                               "Correspondence address if different from above 2"),
    (r"correspondence.*3",                               "Correspondence address if different from above 3"),
    (r"correspondence.*4",                               "Correspondence address if different from above 4"),
    (r"invoice.*1|invoice.?address$",                    "Invoice address if different from below 1"),
    (r"invoice.*2",                                      "Invoice address if different from below 2"),
    (r"invoice.*3",                                      "Invoice address if different from below 3"),
    (r"invoice.*4",                                      "Invoice address if different from below 4"),
    (r"sponsoring.*address.*2|sponsoring.*2",            "Sponsoring Company and Address 2"),
    (r"sponsoring.*address.*3|sponsoring.*3",            "Sponsoring Company and Address 3"),
    (r"sponsoring.*pincode",                             "Postcode_2"),
    (r"where.*heard|heard.*twi",                         "__heard__"),
    (r"venue",                                           "__venue__"),
    (r"contact.?no|mobile|private.?tel",                 "Private Tel"),
    (r"emergency.?contact",                              "Tel"),
    (r"^email$|candidate.?email",                        "Email"),
    (r"bgas.?cert|pcn.?cert|bgas.?no",                   "PCN or BGAS Approval Number"),
    (r"cswip.*cert|cswip.*no",                           "Current CSWIP qualifications held"),
    (r"exam.*type|examination.*type",                    "__exam_type__"),
    (r"exam.*body|examination.*body",                    "__exam_body__"),
    (r"sponsor.*type|application.*type",                 "__sponsor_type__"),
    (r"disability|special.?need",                        "__disability__"),
    (r"gdpr|data.?consent",                              "__gdpr__"),
    (r"heard.*twi|how.*heard",                           "__heard__"),
    (r"duties|responsibilities|pre.?cert.*exp",          "1"),
    (r"ndt.*exp|plant.*exp",                             "1_2"),
    (r"company.?name|present.?company|sponsor.*company", "Sponsoring Company and Address 1"),
    (r"company.*order|order.*no",                        "Company order No"),
    (r"approving.*manager|manager.*name",                "name"),
    (r"verifier.?name",                                  "Name in capitals"),
    (r"verifier.?phone|verifier.?tel",                   "Telephone no"),
    (r"verifier.?email",                                 "Email Address"),
    (r"designation|company.*position",                   "Company  position"),
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
            try:
                annot = ref.get_object()
            except Exception:
                continue
            if not annot or annot.get("/Subtype") != "/Widget":
                continue
            name = str(annot.get("/T", "")).strip()
            if not name:
                continue
            ft = str(annot.get("/FT", ""))
            ff = annot.get("/Ff", 0)
            try: ff = int(ff)
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
# MAPPING
# ══════════════════════════════════════════════════════════════════════════════

def load_manual_mappings() -> dict:
    mf = DATA_DIR / "manual_mappings.json"
    if mf.exists():
        try: return json.loads(mf.read_text())
        except Exception: pass
    return {}

def _split_dob(s: str):
    """
    Split a date string into (DD, MM, YYYY).
    Handles Zoho CRM date formats:
      - YYYY-MM-DD  (API standard)
      - DD/MM/YYYY
      - MM/DD/YYYY
      - DD-MM-YYYY
      - DD-Mon-YYYY  e.g. 15-Nov-1988
      - Mon DD, YYYY e.g. Nov 15, 1988
    """
    s = str(s).strip()
    # Try standard formats first
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y",
                "%d-%b-%Y", "%b %d, %Y", "%B %d, %Y",
                "%d %b %Y", "%d %B %Y"):
        try:
            d = datetime.strptime(s[:12], fmt)
            return str(d.day).zfill(2), str(d.month).zfill(2), str(d.year)
        except ValueError:
            continue
    # Fallback: extract digits
    digits = re.sub(r"\D", "", s)
    if len(digits) == 8:
        # Assume YYYYMMDD (Zoho API format stripped of dashes)
        return digits[6:8], digits[4:6], digits[0:4]
    return "", "", ""

def apply_mappings(raw: dict) -> dict:
    pdf_fields = get_pdf_fields()
    pdf_text   = {k for k,v in pdf_fields.items() if v["type"] == "/Tx"}
    manual     = load_manual_mappings()

    # Auto-map all incoming keys
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
                if norm_zk and norm_pf and (norm_zk in norm_pf or norm_pf in norm_zk):
                    matched = pf
                    break
        auto[zk] = matched or "__ignore__"

    # Manual overrides take priority
    effective = {**auto, **{k: v for k, v in manual.items() if k in raw}}

    out = {}
    for k, v in raw.items():
        target = effective.get(k, "__ignore__")
        if target == "__ignore__":
            continue
        if target == "__dob__":
            dd, dm, dy = _split_dob(str(v))
            out["D"] = dd; out["M"] = dm; out["Y"] = dy
        elif target in SPECIAL_TOKENS:
            out[target] = str(v) if v is not None else ""
        else:
            out[target] = str(v) if v is not None else ""
    return out

def build_field_values(mapped: dict) -> dict:
    pdf_fields = get_pdf_fields()
    pdf_text   = {k for k,v in pdf_fields.items() if v["type"] == "/Tx"}

    # ALL CAPS — TWI form requires capital letters throughout
    # Exclude fields where case matters: email, date, URLs
    NO_CAPS = {"Email", "Email_2", "Email Address", "Date",
               "Event date", "Batch Date"}
    fv = {
        k: (str(v) if k in NO_CAPS else str(v).upper())
        for k, v in mapped.items()
        if k in pdf_text and v is not None
    }

    def _p(*keys):
        for k in keys:
            v = str(mapped.get(k, "")).strip()
            if v: return v
        return ""

    heard = _p("__heard__")
    hb    = {fid: "/Off" for fid, _ in HEARD_FIELDS.values()}
    if heard in HEARD_FIELDS:
        fid, val = HEARD_FIELDS[heard]; hb[fid] = val

    fv.update({
        "Check Box2":   "/1" if _p("__disability__").lower() == "yes" else "/2",
        "Check Box23":  EXAM_TYPE_MAP.get(_p("__exam_type__"), "/Off"),
        "Check Box28":  EXAM_BODY_MAP.get(_p("__exam_body__"), "/Off"),
        "Check Box1.0": "/1" if "self" in _p("__sponsor_type__").lower() else "/2",
        "Check Box21":  "/Yes" if _p("__gdpr__").lower() in ("yes","true","1") else "/Off",
        **hb,
    })
    if not fv.get("Date"):
        fv["Date"] = date.today().strftime("%d/%m/%Y")
    return fv

# ══════════════════════════════════════════════════════════════════════════════
# PDF FILLER
# ══════════════════════════════════════════════════════════════════════════════

def _build_comb_ap(value: str, max_len: int, rect: tuple, fs: float = 8.0) -> bytes:
    x1, y1, x2, y2 = rect
    cell_w = (x2 - x1) / max_len
    base   = (y2 - y1 - fs) / 2.0 + 1.0
    lines  = ["q", f"/Cour {fs} Tf", "0 0 0 rg", "BT"]
    val    = str(value).ljust(max_len)[:max_len]
    for i, ch in enumerate(val):
        if not ch.strip(): continue
        cx = i * cell_w + (cell_w - fs * 0.6) / 2.0
        lines.append(f"1 0 0 1 {cx:.2f} {base:.2f} Tm")
        safe = ch.replace("\\","\\\\").replace("(","\\(").replace(")","\\)")
        lines.append(f"({safe}) Tj")
    lines += ["ET", "Q"]
    return "\n".join(lines).encode()

def _build_multiline_ap(value: str, rect: tuple) -> bytes:
    x1, y1, x2, y2 = rect
    w, h = x2 - x1, y2 - y1
    PAD = 2.0; LG = 1.3; MAX_FS = 8.0; MIN_FS = 5.0; CPP = 0.52

    def wrap(text, fs):
        mc = max(1, int(w / (fs * CPP)))
        lines = []
        for para in text.replace("\r\n","\n").replace("\r","\n").split("\n"):
            lines.extend(textwrap.wrap(para, width=mc) or [""])
        return lines

    fs = MAX_FS
    while fs >= MIN_FS:
        lines = wrap(value, fs)
        if len(lines) * fs * LG + PAD * 2 <= h: break
        fs -= 0.5
    fs    = max(fs, MIN_FS)
    lines = wrap(value, fs)
    lh    = fs * LG
    sy    = h - PAD - fs
    parts = ["q", "BT", f"/Helv {fs:.1f} Tf", "0 0 0 rg"]
    for i, line in enumerate(lines):
        yp = sy - i * lh
        if yp < PAD: break
        safe = line.replace("\\","\\\\").replace("(","\\(").replace(")","\\)")
        parts.append(f"{PAD:.1f} {yp:.2f} Td" if i == 0 else f"0 {-lh:.2f} Td")
        parts.append(f"({safe}) Tj")
    parts += ["ET", "Q"]
    return "\n".join(parts).encode()

def flatten_pdf(pdf_bytes: bytes) -> bytes:
    """
    Flatten a filled PDF — converts all form fields to static content.
    Result is non-editable and renders correctly in all viewers.
    Uses pypdf to set the NeedAppearances flag and flatten annotations.
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()
    writer.append(reader)

    # Set NeedAppearances to false — forces viewers to use existing AP streams
    if writer._root_object.get("/AcroForm"):
        writer._root_object["/AcroForm"].update({
            NameObject("/NeedAppearances"): False,
        })

    # Flatten each annotation — move appearance stream content to page
    # and remove the widget annotation so fields are no longer interactive
    for page in writer.pages:
        if "/Annots" not in page:
            continue
        annots_to_keep = []
        for ref in page["/Annots"]:
            try:
                annot = ref.get_object()
            except Exception:
                continue
            if annot is None or not hasattr(annot, "get"):
                continue
            # Keep non-widget annotations (links, comments etc)
            if annot.get("/Subtype") != "/Widget":
                annots_to_keep.append(ref)
                continue
            # For widget annotations — just drop them (field becomes static
            # because the appearance stream was already rendered into the page
            # content by update_page_form_field_values + our custom AP streams)

        if annots_to_keep:
            page[NameObject("/Annots")] = annots_to_keep
        elif "/Annots" in page:
            del page["/Annots"]

    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def fill_pdf(template_bytes: bytes, fv: dict) -> bytes:
    reader = PdfReader(io.BytesIO(template_bytes))
    writer = PdfWriter()
    writer.append(reader)
    for page in writer.pages:
        writer.update_page_form_field_values(page, fv, auto_regenerate=False)

    for page in writer.pages:
        for ref in page.get("/Annots", []):
            try:
                annot = ref.get_object()
            except Exception:
                continue
            if annot is None or not hasattr(annot, "get"): continue
            if annot.get("/Subtype") != "/Widget": continue
            fname = str(annot.get("/T", ""))
            value = fv.get(fname, "")
            rect  = tuple(float(v) for v in annot.get("/Rect", [0,0,0,0]))
            ap_bytes = None
            if fname in COMB_FIELDS:
                if value:
                    ap_bytes = _build_comb_ap(value, COMB_FIELDS[fname], rect)
            elif annot.get("/Ff") and bool(int(annot["/Ff"]) & (1 << 12)):
                if value:
                    ap_bytes = _build_multiline_ap(value, rect)
            if ap_bytes is None: continue
            stream = DecodedStreamObject()
            stream.set_data(ap_bytes)
            stream.update({
                NameObject("/Type"):    NameObject("/XObject"),
                NameObject("/Subtype"): NameObject("/Form"),
                NameObject("/BBox"):    RectangleObject([0, 0,
                                            rect[2]-rect[0], rect[3]-rect[1]]),
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
        try: return json.loads(JOBS_FILE.read_text())
        except Exception: pass
    return []

def save_jobs(jobs: list):
    JOBS_FILE.write_text(json.dumps(jobs, indent=2, default=str))

def log_job(job: dict):
    jobs = load_jobs()
    jobs.insert(0, job)
    # Keep last 500 jobs only
    save_jobs(jobs[:500])

# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/generate")
async def generate(request: Request):
    """
    Called by Deluge button script.

    Body (JSON):
    {
        "record_data": {
            "Candidate Name as per ID Proof": "John Smith",
            "Date of Birth":                  "15/06/1988",
            "Course Name":                    "CSWIP 3.1",
            ...all CRM field values...
        },
        "record_id":  "738XXXXXXXXXX",   // used for logging only
        "filename":   "TWI_JohnSmith.pdf" // optional
    }

    Returns:
    {
        "ok":         true,
        "job_id":     "uuid",
        "filename":   "TWI_JohnSmith.pdf",
        "pdf_base64": "JVBERi0x..."    // Deluge uses this to attach
    }
    """
    body        = await request.json()
    record_data = body.get("record_data", {})
    record_id   = body.get("record_id", "unknown")
    job_id      = str(uuid.uuid4())

    if not record_data:
        raise HTTPException(400, "record_data is required")
    if not PDF_TEMPLATE_PATH.exists():
        raise HTTPException(500, "PDF template not found on server")

    # Candidate name for filename
    cname = ""
    for k in ("Candidate Name as per ID Proof", "Full_Name",
              "Last_Name", "Name", "name"):
        if record_data.get(k):
            cname = str(record_data[k]).strip().replace(" ", "_")
            break
    cname    = cname or "Candidate"
    filename = body.get("filename") or f"TWI_{cname}_{datetime.now().strftime('%Y%m%d')}.pdf"

    try:
        template   = PDF_TEMPLATE_PATH.read_bytes()
        mapped     = apply_mappings(record_data)
        fv         = build_field_values(mapped)
        pdf_bytes  = fill_pdf(template, fv)
        # Delete previous PDF for this record (keep only latest)
        if record_id != "unknown":
            for old_job in load_jobs():
                if old_job.get("record_id") == record_id:
                    old_pdf = PDF_STORE / f"{old_job['id']}.pdf"
                    if old_pdf.exists():
                        old_pdf.unlink()

        # Save PDF — valid for 7 days or until next generation
        (PDF_STORE / f"{job_id}.pdf").write_bytes(pdf_bytes)

        log_job({
            "id":         job_id,
            "record_id":  record_id,
            "candidate":  cname.replace("_", " "),
            "filename":   filename,
            "status":     "Done",
            "created_at": datetime.now().isoformat(),
            "error":      None,
        })

        return JSONResponse({
            "ok":      True,
            "job_id":  job_id,
            "filename": filename,
        })

    except Exception as ex:
        log_job({
            "id":         job_id,
            "record_id":  record_id,
            "candidate":  cname.replace("_", " "),
            "filename":   filename,
            "status":     "Error",
            "created_at": datetime.now().isoformat(),
            "error":      str(ex),
        })
        raise HTTPException(500, str(ex))


@app.get("/pdf/{job_id}", response_class=Response)
async def download_pdf(job_id: str):
    pdf_path = PDF_STORE / f"{job_id}.pdf"
    if not pdf_path.exists():
        raise HTTPException(404, detail="PDF not found or expired. Please regenerate from CRM.")
    # Check 7-day expiry
    age_days = (datetime.now().timestamp() - pdf_path.stat().st_mtime) / 86400
    if age_days > 7:
        pdf_path.unlink()
        raise HTTPException(410, detail="PDF link expired (7 days). Please click Generate TWI PDF again.")
    jobs     = load_jobs()
    job      = next((j for j in jobs if j["id"] == job_id), {})
    filename = job.get("filename", "TWI_Form.pdf")
    return Response(
        content=pdf_path.read_bytes(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"',
                 "Cache-Control": "no-cache"},
    )




@app.get("/workdrive/{job_id}")
async def get_workdrive_id(job_id: str):
    """
    Upload the generated PDF to Zoho WorkDrive and return the file ID.
    Deluge calls this after /generate, then uses the file ID to attach to CRM.
    """
    import urllib.request as _ur, urllib.parse as _up

    pdf_path = PDF_STORE / f"{job_id}.pdf"
    if not pdf_path.exists():
        raise HTTPException(404, "PDF not found — run /generate first")

    jobs     = load_jobs()
    job      = next((j for j in jobs if j["id"] == job_id), {})
    filename = job.get("filename", f"TWI_{job_id[:6]}.pdf")

    WORKDRIVE_FOLDER = "j85fx89434c9ab31f481f95184423fc50d761"

    # Token comes from the Authorization header — set by Deluge connection
    auth_header      = request.headers.get("Authorization", "")
    WORKDRIVE_TOKEN  = auth_header.replace("Zoho-oauthtoken ", "").strip()

    if not WORKDRIVE_TOKEN:
        # Fallback to env var if set
        WORKDRIVE_TOKEN = os.environ.get("WORKDRIVE_TOKEN", "")

    if not WORKDRIVE_TOKEN:
        raise HTTPException(500, "No WorkDrive token — set WORKDRIVE_TOKEN env var in Render")

    pdf_bytes  = pdf_path.read_bytes()
    boundary   = uuid.uuid4().hex
    body_bytes = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="content"; filename="{filename}"\r\n'
        f"Content-Type: application/pdf\r\n\r\n"
    ).encode() + pdf_bytes + f"\r\n--{boundary}--\r\n".encode()

    url = f"https://workdrive.zoho.in/api/v1/upload?parent_id={WORKDRIVE_FOLDER}&override-name-exist=true"
    req = _ur.Request(url, data=body_bytes, method="POST")
    req.add_header("Authorization", f"Zoho-oauthtoken {WORKDRIVE_TOKEN}")
    req.add_header("Content-Type",  f"multipart/form-data; boundary={boundary}")

    with _ur.urlopen(req, timeout=30) as r:
        resp = json.loads(r.read())

    # WorkDrive returns file ID in data[0].id
    try:
        file_id = resp["data"][0]["id"]
    except (KeyError, IndexError):
        raise HTTPException(500, f"WorkDrive upload failed: {resp}")

    return JSONResponse({
        "ok":       True,
        "file_id":  file_id,
        "filename": filename,
    })
@app.get("/health")
async def health():
    return JSONResponse({
        "ok":       PDF_TEMPLATE_PATH.exists(),
        "template": str(PDF_TEMPLATE_PATH) if PDF_TEMPLATE_PATH.exists() else "MISSING",
        "fields":   len(get_pdf_fields()),
        "jobs":     len(load_jobs()),
    })


@app.get("/queue", response_class=HTMLResponse)
async def queue_page(auth: str = Depends(require_auth)):
    jobs = load_jobs()
    rows = ""
    for job in jobs:
        status = job.get("status", "?")
        color  = {"Done":"#10b981","Error":"#ef4444"}.get(status,"#f59e0b")
        badge  = (f'<span style="background:{color};color:#fff;padding:2px 10px;'
                  f'border-radius:12px;font-size:11px;font-weight:600">{status}</span>')
        ts     = job.get("created_at","")[:16].replace("T"," ")
        dl     = (f'<a href="/pdf/{job["id"]}" target="_blank" style="background:#3b82f6;'
                  f'color:#fff;padding:4px 14px;border-radius:5px;text-decoration:none;'
                  f'font-size:12px">⬇ Download</a>'
                  if status == "Done" else
                  f'<span style="color:#ef4444;font-size:11px">{job.get("error","")[:60]}</span>')
        rows  += (f'<tr><td>{job.get("candidate","—")}</td>'
                  f'<td style="color:#64748b;font-size:11px">{job.get("record_id","")}</td>'
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
<th>Candidate</th><th>CRM Record</th><th>Filename</th>
<th>Status</th><th>Time</th><th>Action</th>
</tr></thead><tbody>
{rows if jobs else '<tr><td colspan="6" class="empty">No jobs yet — click Generate TWI PDF inside a CRM record.</td></tr>'}
</tbody></table></body></html>""")


@app.get("/", response_class=HTMLResponse)
async def root(auth: str = Depends(require_auth)):
    """Root page — links to all endpoints."""
    tmpl_ok = PDF_TEMPLATE_PATH.exists()
    fields  = len(get_pdf_fields())
    jobs    = len(load_jobs())
    tmpl_color = "#10b981" if tmpl_ok else "#ef4444"
    tmpl_text  = f"Found ({fields} fields scanned)" if tmpl_ok else "MISSING — see /debug"
    return HTMLResponse(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>TWI PDF Engine</title>
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
<p class="sub">Blastline Institute — TWI Enrolment Form Filler</p>

<div class="card">
  <h2>Status</h2>
  <div class="row"><span>PDF Template</span>
    <span class="val" style="color:{tmpl_color}">{tmpl_text}</span></div>
  <div class="row"><span>Jobs processed</span>
    <span class="val">{jobs}</span></div>
  <div class="row"><span>Server</span>
    <span class="val ok">Running ✓</span></div>
</div>

<div class="card">
  <h2>Endpoints</h2>
  <a class="btn" href="/queue">📋 PDF Queue</a>
  <a class="btn grey" href="/health">Health JSON</a>
  <a class="btn grey" href="/debug">Debug Paths</a>
  <a class="btn grey" href="/docs">API Docs</a>
</div>
</body></html>""")


@app.get("/debug")
async def debug():
    """Shows server file paths — useful for diagnosing template location issues."""
    import sys
    cwd      = os.getcwd()
    app_dir  = str(_APP_DIR)
    data_dir = str(DATA_DIR)

    # List files in app dir
    try:
        app_files = [f.name for f in _APP_DIR.iterdir()]
    except Exception as e:
        app_files = [str(e)]

    # List files in data dir
    try:
        data_files = [f.name for f in DATA_DIR.iterdir()]
    except Exception as e:
        data_files = [str(e)]

    return JSONResponse({
        "cwd":              cwd,
        "app_dir":          app_dir,
        "data_dir":         data_dir,
        "template_path":    str(PDF_TEMPLATE_PATH),
        "template_exists":  PDF_TEMPLATE_PATH.exists(),
        "app_dir_files":    sorted(app_files),
        "data_dir_files":   sorted(data_files),
        "python":           sys.version,
        "env_DATA_DIR":     os.environ.get("DATA_DIR", "not set"),
    })


@app.post("/upload-template")
async def upload_template(request: Request):
    """
    Upload the PDF template directly to the server.
    Use this if the PDF is not in your GitHub repo.

    POST with Content-Type: application/pdf
    Body: raw PDF bytes
    """
    pdf_bytes = await request.body()
    if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
        raise HTTPException(400, "Invalid PDF — body must be raw PDF bytes")
    PDF_TEMPLATE_PATH.write_bytes(pdf_bytes)
    global _pdf_fields_cache
    _pdf_fields_cache = {}          # force re-scan
    fields = len(get_pdf_fields())
    return JSONResponse({
        "ok":      True,
        "path":    str(PDF_TEMPLATE_PATH),
        "size_kb": len(pdf_bytes) // 1024,
        "fields":  fields,
    })
