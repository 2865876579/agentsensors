import sys; sys.path.insert(0,".")
from datetime import date
from email.utils import parsedate_to_datetime
import imaplib, email
from config import EMAIL_HOST, EMAIL_USER, EMAIL_PASS

imap = imaplib.IMAP4_SSL(EMAIL_HOST, timeout=10)
tag = imap._new_tag()
imap.send(tag + b" ID ("name" "SmartPillow" "version" "1.0")
")
imap._get_tagged_response(tag, "ID")
imap.login(EMAIL_USER, EMAIL_PASS)
imap.select("INBOX", readonly=True)
today = date.today()
print("Today:", today)
st, ids = imap.search(None, "ALL")
for mid in ids[0].split()[-15:]:
    st, data = imap.fetch(mid, "(BODY[HEADER.FIELDS (DATE SUBJECT)])")
    if st == "OK" and data[0]:
        raw = data[0][1]
        msg = email.message_from_bytes(raw)
        ds = msg.get("Date","")
        subj = msg.get("Subject","")
        try:
            dt = parsedate_to_datetime(ds)
            match = "MATCH" if dt.date() == today else "skip"
            print(match, dt.date(), subj[:60])
        except Exception as e:
            print("PARSE_FAIL", e, ds[:50])
imap.logout()
