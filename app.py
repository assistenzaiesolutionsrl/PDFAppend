from flask import Flask, request, jsonify
import pikepdf
import base64
import io
import os
import hashlib
import hmac
import tempfile
import shutil
import traceback

app = Flask(__name__)

SHARED_SECRET = os.environ.get("PYHANKO_SECRET", "")


def verify_secret(req_secret):
    if not SHARED_SECRET:
        return True
    return hmac.compare_digest(req_secret or "", SHARED_SECRET)


def get_docmdp_permission(pdf):
    """
    Legge il livello di permesso DocMDP dal PDF firmato:
      1 = nessuna modifica consentita dopo la firma
      2 = solo compilazione campi form e aggiunta firme
      3 = annotazioni, form e firme consentite
      None = nessuna restrizione DocMDP presente
    Fonte: PDF spec ISO 32000, sezione 12.8.2.2
    """
    try:
        # Percorso standard DocMDP: /Root/Perms/DocMDP
        catalog = pdf.Root
        if "/Perms" in catalog:
            perms = catalog["/Perms"]
            if "/DocMDP" in perms:
                sig = perms["/DocMDP"]
                if "/Reference" in sig:
                    for ref in sig["/Reference"]:
                        tm = ref.get("/TransformMethod")
                        if tm == pikepdf.Name("/DocMDP"):
                            params = ref.get("/TransformParams")
                            if params and "/P" in params:
                                return int(params["/P"])
    except Exception as ex:
        print(f"DocMDP read warning: {ex}", flush=True)
    return None  # nessuna restrizione rilevata


def add_visual_stamp(pdf, stamp_info, page_index):
    """
    Aggiunge il timbro FEA come Annotation (Appearance Stream).
    Non tocca il content stream originale della pagina — solo /Annots.
    """
    stamp_bytes = base64.b64decode(stamp_info["stamp_pdf_b64"])

    if page_index >= len(pdf.pages):
        page_index = len(pdf.pages) - 1

    page = pdf.pages[page_index]

    with pikepdf.open(io.BytesIO(stamp_bytes)) as stamp_doc:
        stamp_page = stamp_doc.pages[0]

        # Estrai content stream del timbro
        content_bytes = b""
        if "/Contents" in stamp_page.obj:
            contents = stamp_page.obj["/Contents"]
            if isinstance(contents, pikepdf.Array):
                for s in contents:
                    content_bytes += s.read_raw_bytes()
            else:
                content_bytes = contents.read_raw_bytes()

        w = float(stamp_page.mediabox[2])
        h = float(stamp_page.mediabox[3])

        # Crea Form XObject con il contenuto grafico del timbro
        xobj = pdf.make_stream(content_bytes)
        xobj["/Type"] = pikepdf.Name("/XObject")
        xobj["/Subtype"] = pikepdf.Name("/Form")
        xobj["/BBox"] = pikepdf.Array([0, 0, w, h])

        if "/Resources" in stamp_page.obj:
            xobj["/Resources"] = pdf.copy_foreign(
                stamp_doc.make_indirect(stamp_page.obj["/Resources"])
            )

        # Assicura che Resources/XObject esista sulla pagina
        if "/Resources" not in page:
            page["/Resources"] = pikepdf.Dictionary()
        if "/XObject" not in page["/Resources"]:
            page["/Resources"]["/XObject"] = pikepdf.Dictionary()

        xobj_name = pikepdf.Name(f"/FeaStamp{page_index}")
        page["/Resources"]["/XObject"][xobj_name] = xobj

        rect_x = float(stamp_info.get("x", 0))
        rect_y = float(stamp_info.get("y", 0))

        # Annotation Stamp — non modifica il content stream originale
        annotation = pikepdf.Dictionary(
            Type=pikepdf.Name("/Annot"),
            Subtype=pikepdf.Name("/Stamp"),
            Rect=pikepdf.Array([rect_x, rect_y, rect_x + w, rect_y + h]),
            AP=pikepdf.Dictionary(N=xobj),
            F=4,  # Print flag: visibile e stampabile
            NM=pikepdf.String(f"FEA-Stamp-{page_index}"),
            Contents=pikepdf.String("FEA - Firma Elettronica Avanzata"),
        )

        if "/Annots" not in page.obj:
            page.obj["/Annots"] = pikepdf.Array()
        page.obj["/Annots"].append(pdf.make_indirect(annotation))


def add_attachment(pdf, att):
    """Aggiunge un file allegato al PDF (es. timestamp.tsr, audit_chain.txt)."""
    att_bytes = base64.b64decode(att["b64"])
    att_name = att["name"]
    att_mime = att.get("mime", "application/octet-stream")

    ef_stream = pdf.make_stream(att_bytes)
    ef_stream["/Type"] = pikepdf.Name("/EmbeddedFile")
    ef_stream["/Subtype"] = pikepdf.Name(
        "/" + att_mime.replace("/", "#2F").replace(";", "#3B")
    )
    ef_stream["/Params"] = pikepdf.Dictionary(
        Size=len(att_bytes),
        CheckSum=hashlib.md5(att_bytes).digest(),
    )

    fs_dict = pikepdf.Dictionary(
        Type=pikepdf.Name("/Filespec"),
        F=att_name,
        UF=att_name,
        EF=pikepdf.Dictionary(F=ef_stream),
        Desc=att.get("description", ""),
    )

    catalog = pdf.Root
    if "/Names" not in catalog:
        catalog["/Names"] = pikepdf.Dictionary()
    if "/EmbeddedFiles" not in catalog["/Names"]:
        catalog["/Names"]["/EmbeddedFiles"] = pikepdf.Dictionary(
            Names=pikepdf.Array()
        )

    names_arr = catalog["/Names"]["/EmbeddedFiles"]["/Names"]
    names_arr.append(pikepdf.String(att_name))
    names_arr.append(fs_dict)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "pyhanko-seal"})


@app.route("/seal", methods=["POST"])
def seal():
    tmp_orig_path = None
    output_path = None
    docmdp = None
    visual_stamp_added = False

    try:
        data = request.get_json(force=True)

        if not verify_secret(data.get("secret", "")):
            return jsonify({"error": "Unauthorized"}), 401

        original_b64 = data.get("original_pdf_b64", "")
        if not original_b64:
            return jsonify({"error": "Missing original_pdf_b64"}), 400

        original_bytes = base64.b64decode(original_b64)
        stamps     = data.get("stamps", [])
        attachments = data.get("attachments", [])

        # Scrivi PDF originale su file temporaneo
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_orig:
            tmp_orig.write(original_bytes)
            tmp_orig_path = tmp_orig.name

        output_path = tmp_orig_path + "_sealed.pdf"

        # ── STEP 1: Leggi DocMDP PRIMA di aprire in write mode ──────────────
        with pikepdf.open(tmp_orig_path) as pdf_ro:
            docmdp = get_docmdp_permission(pdf_ro)

        print(f"DocMDP level rilevato: {docmdp}", flush=True)

        # Regole DocMDP:
        # None o 3 → annotazioni consentite → aggiungi timbro visivo
        # 2        → solo form/firme → skip timbro, solo allegati
        # 1        → nessuna modifica → skip timbro, solo allegati
        can_add_annotations = (docmdp is None or docmdp >= 3)

        if not can_add_annotations:
            print(
                f"DocMDP={docmdp}: timbro visivo NON aggiunto per preservare FEQ. "
                f"Aggiungo solo allegati.",
                flush=True
            )

        # ── STEP 2: Copia byte-per-byte — i byte 0→N non vengono mai riscritti
        shutil.copy2(tmp_orig_path, output_path)

        # ── STEP 3: Apri la copia e applica modifiche incrementali ──────────
        with pikepdf.open(output_path, allow_overwriting_input=True) as pdf:

            if can_add_annotations:
                for stamp_info in stamps:
                    page_index = stamp_info.get("page_index", 0)
                    add_visual_stamp(pdf, stamp_info, page_index)
                    visual_stamp_added = True
                    print(f"Timbro FEA aggiunto a pagina {page_index}", flush=True)

            for att in attachments:
                add_attachment(pdf, att)
                print(f"Allegato aggiunto: {att.get('name')}", flush=True)

            # ── STEP 4: Salva in modalità INCREMENTALE ─────────────────────
            # incremental=True → appende solo i nuovi oggetti in coda al file.
            # I byte originali (0→N) rimangono INTATTI → FEQ resta valida.
            pdf.save(
                output_path,
                linearize=False,
                compress_streams=False,
                incremental=True,
            )

        with open(output_path, "rb") as f:
            signed_bytes = f.read()

        print(
            f"Seal completato. "
            f"Dimensione originale: {len(original_bytes)}b → "
            f"Risultato: {len(signed_bytes)}b "
            f"(+{len(signed_bytes)-len(original_bytes)}b incremento)",
            flush=True
        )

        return jsonify({
            "signed_pdf_b64":    base64.b64encode(signed_bytes).decode(),
            "docmdp_level":      docmdp,
            "visual_stamp_added": visual_stamp_added,
            "original_size":     len(original_bytes),
            "sealed_size":       len(signed_bytes),
        })

    except Exception as e:
        print("SEAL ERROR:", traceback.format_exc(), flush=True)
        return jsonify({
            "error":     str(e),
            "traceback": traceback.format_exc()
        }), 500

    finally:
        if tmp_orig_path:
            try:
                os.unlink(tmp_orig_path)
            except Exception:
                pass
        if output_path:
            try:
                os.unlink(output_path)
            except Exception:
                pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
