"""
app.py — pyhanko-seal microservice
Aggiunge timbri visivi e allegati ai PDF tramite incremental update.
Preserva le firme digitali esistenti (FEQ).
"""
import io
import os
import base64
import traceback
import pikepdf
from flask import Flask, request, jsonify
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.pdf_utils import generic

app = Flask(__name__)
SECRET = os.environ.get('SECRET', '')

# ─── Helpers PDF ──────────────────────────────────────────────────────────────

def _n(name):
    """Crea un NameObject pyhanko da stringa /Nome."""
    return generic.NameObject(name)


def _str(s):
    """Crea una stringa PDF pyhanko."""
    return generic.pdf_string(s)


def _get_content_bytes(obj):
    """Estrae i byte decodificati da un Content stream (singolo o array)."""
    if isinstance(obj, pikepdf.Array):
        data = b''
        for item in obj:
            data += bytes(item.read_bytes())
        return data
    return bytes(obj.read_bytes())


def _pikepdf_val_to_generic(val, writer, src_doc):
    """Converte ricorsivamente un valore pikepdf in un oggetto pyhanko/generic."""
    if isinstance(val, pikepdf.Dictionary):
        d = generic.DictionaryObject()
        for k, v in val.items():
            d[str(k)] = _pikepdf_val_to_generic(v, writer, src_doc)
        return d
    elif isinstance(val, pikepdf.Array):
        return generic.ArrayObject([_pikepdf_val_to_generic(i, writer, src_doc) for i in val])
    elif isinstance(val, pikepdf.Name):
        return generic.NameObject(str(val))
    elif isinstance(val, pikepdf.String):
        return generic.pdf_string(bytes(val))
    elif isinstance(val, pikepdf.Stream):
        data = bytes(val.read_bytes())
        d = {}
        for k, v in val.items():
            if str(k) in ('/Length', '/Filter', '/DecodeParms'):
                continue
            d[str(k)] = _pikepdf_val_to_generic(v, writer, src_doc)
        s = generic.StreamObject(stream_data=data, dict_data=d)
        return writer.add_object(s)
    elif isinstance(val, bool):
        return generic.BooleanObject(val)
    elif isinstance(val, int):
        return generic.NumberObject(val)
    elif isinstance(val, float):
        return generic.FloatObject(val)
    else:
        return generic.NullObject()


def _build_resources(writer, stamp_page_obj, src_doc):
    """Costruisce il dict /Resources per il Form XObject del timbro."""
    if '/Resources' not in stamp_page_obj:
        return generic.DictionaryObject()

    src_res = stamp_page_obj['/Resources']
    res = generic.DictionaryObject()

    for key in ('/Font', '/ExtGState', '/ColorSpace', '/Pattern', '/Shading'):
        if key not in src_res:
            continue
        sub = generic.DictionaryObject()
        for name, obj in src_res[key].items():
            if isinstance(obj, pikepdf.Stream):
                converted = _pikepdf_val_to_generic(obj, writer, src_doc)
            else:
                d = generic.DictionaryObject()
                if isinstance(obj, pikepdf.Dictionary):
                    for k, v in obj.items():
                        d[str(k)] = _pikepdf_val_to_generic(v, writer, src_doc)
                    converted = writer.add_object(d)
                else:
                    converted = _pikepdf_val_to_generic(obj, writer, src_doc)
            sub[str(name)] = converted
        res[key] = sub

    # XObject annidati
    if '/XObject' in src_res:
        xsub = generic.DictionaryObject()
        for name, obj in src_res['/XObject'].items():
            if isinstance(obj, pikepdf.Stream):
                xsub[str(name)] = _pikepdf_val_to_generic(obj, writer, src_doc)
            else:
                xsub[str(name)] = _pikepdf_val_to_generic(obj, writer, src_doc)
        res['/XObject'] = xsub

    return res


def _get_page_refs(writer):
    """
    Restituisce lista di reference pyhanko per ogni pagina del documento.
    Gestisce alberi di pagine piatti e annidati.
    """
    refs = []

    def walk(node_val):
        if hasattr(node_val, 'get_object'):
            node = node_val.get_object()
        else:
            node = node_val

        obj_type = node.get('/Type', generic.NameObject(''))
        if obj_type == generic.NameObject('/Page'):
            refs.append(node_val)
        elif obj_type == generic.NameObject('/Pages'):
            kids = node.get('/Kids', generic.ArrayObject())
            for kid in kids:
                walk(kid)

    root = writer.prev.root
    pages_val = root.raw_get('/Pages')
    walk(pages_val)
    return refs


def _add_stamp(writer, page_index, stamp_pdf_bytes):
    """Aggiunge un timbro come annotazione Stamp con appearance stream sul documento."""
    stamp_doc = pikepdf.open(io.BytesIO(stamp_pdf_bytes))
    stamp_page = stamp_doc.pages[0]

    bbox = [float(x) for x in stamp_page.mediabox]
    pw = bbox[2] - bbox[0]
    ph = bbox[3] - bbox[1]

    # Estrai content bytes
    content_bytes = b''
    if '/Contents' in stamp_page.obj:
        content_bytes = _get_content_bytes(stamp_page.obj['/Contents'])

    # Costruisci risorse
    resources = _build_resources(writer, stamp_page.obj, stamp_doc)

    # Crea Form XObject (appearance stream)
    xobj = generic.StreamObject(
        stream_data=content_bytes,
        dict_data={
            '/Type':     _n('/XObject'),
            '/Subtype':  _n('/Form'),
            '/FormType': generic.NumberObject(1),
            '/BBox':     generic.ArrayObject([
                generic.FloatObject(0), generic.FloatObject(0),
                generic.FloatObject(pw), generic.FloatObject(ph),
            ]),
            '/Resources': resources,
        }
    )
    xobj_ref = writer.add_object(xobj)

    # Crea annotazione Stamp
    annot = generic.DictionaryObject({
        '/Type':    _n('/Annot'),
        '/Subtype': _n('/Stamp'),
        '/Rect':    generic.ArrayObject([
            generic.FloatObject(0), generic.FloatObject(0),
            generic.FloatObject(pw), generic.FloatObject(ph),
        ]),
        '/F':  generic.NumberObject(4),
        '/AP': generic.DictionaryObject({'/N': xobj_ref}),
    })
    annot_ref = writer.add_object(annot)

    # Aggiungi annotazione alla pagina
    page_refs = _get_page_refs(writer)
    if page_index >= len(page_refs):
        page_index = len(page_refs) - 1
    page_ref = page_refs[page_index]
    page_obj = writer.get_object(page_ref.reference if hasattr(page_ref, 'reference') else page_ref)

    if '/Annots' in page_obj:
        raw_annots = page_obj.raw_get('/Annots')
        if isinstance(raw_annots, generic.IndirectObject):
            annots_obj = writer.get_object(raw_annots.reference)
            annots_obj.append(annot_ref)
            writer.mark_update(raw_annots.reference)
        else:
            page_obj['/Annots'].append(annot_ref)
            _mark_page(writer, page_ref)
    else:
        page_obj['/Annots'] = generic.ArrayObject([annot_ref])
        _mark_page(writer, page_ref)


def _mark_page(writer, page_ref):
    ref = page_ref.reference if hasattr(page_ref, 'reference') else page_ref
    writer.mark_update(ref)


def _add_attachment(writer, att):
    """Aggiunge un file allegato al PDF tramite incremental update."""
    name = att['name']
    data = base64.b64decode(att['b64'])
    mime = att.get('mime', 'application/octet-stream')
    desc = att.get('description', '')

    ef_stream = generic.StreamObject(
        stream_data=data,
        dict_data={
            '/Type':    _n('/EmbeddedFile'),
            '/Params':  generic.DictionaryObject({
                '/Size': generic.NumberObject(len(data)),
            }),
        }
    )
    ef_ref = writer.add_object(ef_stream)

    filespec = generic.DictionaryObject({
        '/Type': _n('/Filespec'),
        '/F':    _str(name),
        '/UF':   _str(name),
        '/Desc': _str(desc),
        '/EF':   generic.DictionaryObject({'/F': ef_ref}),
    })
    fs_ref = writer.add_object(filespec)

    root_ref = writer.prev.root.container_ref
    root = writer.get_object(root_ref.reference)

    if '/Names' not in root:
        root['/Names'] = generic.DictionaryObject()
    names = root['/Names']

    if '/EmbeddedFiles' not in names:
        names['/EmbeddedFiles'] = generic.DictionaryObject({
            '/Names': generic.ArrayObject()
        })

    names['/EmbeddedFiles']['/Names'].append(_str(name))
    names['/EmbeddedFiles']['/Names'].append(fs_ref)
    writer.mark_update(root_ref.reference)


# ─── Route principale ────────────────────────────────────────────────────────

@app.route('/health')
def health():
    return jsonify({"service": "pyhanko-seal", "status": "ok"})


@app.route('/seal', methods=['POST'])
def seal():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Missing JSON body"}), 400

        if SECRET and data.get('secret', '') != SECRET:
            return jsonify({"error": "Unauthorized"}), 401

        original_bytes = base64.b64decode(data['original_pdf_b64'])
        stamps = data.get('stamps', [])
        attachments = data.get('attachments', [])

        input_buf = io.BytesIO(original_bytes)
        output_buf = io.BytesIO()

        writer = IncrementalPdfFileWriter(input_buf, strict=False)

        for stamp_data in stamps:
            _add_stamp(writer, stamp_data['page_index'], base64.b64decode(stamp_data['stamp_pdf_b64']))

        for att in attachments:
            _add_attachment(writer, att)

        writer.write(output_buf)
        result_bytes = output_buf.getvalue()

        return jsonify({"signed_pdf_b64": base64.b64encode(result_bytes).decode()})

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[SEAL ERROR] {e}\n{tb}", flush=True)
        return jsonify({"error": str(e), "traceback": tb}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
