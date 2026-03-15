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


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "pyhanko-seal"})


@app.route("/seal", methods=["POST"])
def seal():
    tmp_orig_path = None
    output_path = None
    try:
        data = request.get_json(force=True)

        if not verify_secret(data.get("secret", "")):
            return jsonify({"error": "Unauthorized"}), 401

        original_b64 = data.get("original_pdf_b64", "")
        if not original_b64:
            return jsonify({"error": "Missing original_pdf_b64"}), 400

        original_bytes = base64.b64decode(original_b64)
        stamps = data.get("stamps", [])
        attachments = data.get("attachments", [])

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_orig:
            tmp_orig.write(original_bytes)
            tmp_orig_path = tmp_orig.name

        output_path = tmp_orig_path + "_sealed.pdf"

        # Copia byte-per-byte l'originale — i byte 0→N non verranno mai toccati
        shutil.copy2(tmp_orig_path, output_path)

        with pikepdf.open(output_path, allow_overwriting_input=True) as pdf:

            for stamp_info in stamps:
                page_index = stamp_info.get("page_index", 0)
                stamp_bytes = base64.b64decode(stamp_info["stamp_pdf_b64"])

                if page_index >= len(pdf.pages):
                    page_index = len(pdf.pages) - 1

                page = pdf.pages[page_index]

                if "/Resources" not in page:
                    page["/Resources"] = pikepdf.Dictionary()
                if "/XObject" not in page["/Resources"]:
                    page["/Resources"]["/XObject"] = pikepdf.Dictionary()

                with pikepdf.open(io.BytesIO(stamp_bytes)) as stamp_doc:
                    stamp_page = stamp_doc.pages[0]

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

                    xobj = pdf.make_stream(content_bytes)
                    xobj["/Type"] = pikepdf.Name("/XObject")
                    xobj["/Subtype"] = pikepdf.Name("/Form")
                    xobj["/BBox"] = pikepdf.Array([0, 0, w, h])

                    if "/Resources" in stamp_page.obj:
                        resources = stamp_page.obj["/Resources"]
                        # copy_foreign richiede un oggetto indiretto
                        xobj["/Resources"] = stamp_doc.copy_foreign(
                            stamp_doc.make_indirect(resources)
                        )

                    xobj_name = pikepdf.Name(f"/Stamp{page_index}")
                    page["/Resources"]["/XObject"][xobj_name] = xobj

                    # Coordinate del timbro in punti PDF (dal chiamante, default 0,0)
                    rect_x = float(stamp_info.get("x", 0))
                    rect_y = float(stamp_info.get("y", 0))

                    # Annotation di tipo Stamp con Appearance Stream.
                    # Il content stream originale NON viene mai toccato:
                    # la FEQ preesistente rimane valida.
                    annotation = pikepdf.Dictionary(
                        Type=pikepdf.Name("/Annot"),
                        Subtype=pikepdf.Name("/Stamp"),
                        Rect=pikepdf.Array([
                            rect_x,
                            rect_y,
                            rect_x + w,
                            rect_y + h,
                        ]),
                        AP=pikepdf.Dictionary(
                            N=xobj  # Normal appearance = il nostro Form XObject
                        ),
                        F=pikepdf.Integer(4),  # Print flag
                        NM=pikepdf.String(f"FEA-Stamp-{page_index}"),
                        Contents=pikepdf.String("FEA - Firma Elettronica Avanzata"),
                    )

                    if "/Annots" not in page.obj:
                        page.obj["/Annots"] = pikepdf.Array()
                    page.obj["/Annots"].append(pdf.make_indirect(annotation))

            for att in attachments:
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

            # ✅ incremental=True — appende solo i nuovi oggetti in coda.
            # I byte originali (0→N) rimangono intatti:
            # le firme FEQ preesistenti restano valide.
            pdf.save(
                output_path,
                linearize=False,
                recompress_streams=False,
                fix_metadata_version=False,
                incremental=True,
            )

        with open(output_path, "rb") as f:
            signed_bytes = f.read()

        return jsonify({"signed_pdf_b64": base64.b64encode(signed_bytes).decode()})

    except Exception as e:
        # Logga il dettaglio completo nei log Railway (visibile nella tab Logs)
        print("SEAL ERROR:", traceback.format_exc(), flush=True)
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500

    finally:
        # Pulizia file temporanei — sempre, anche in caso di errore
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
