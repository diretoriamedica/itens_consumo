import os
from flask import Flask, jsonify, request
from supabase import create_client, Client

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None


def sb():
    return supabase


def apply_filters(query, filters):
    for col, val in filters:
        query = query.eq(col, val)
    return query


@app.route("/")
@app.route("/api/")
def index():
    return jsonify({
        "status": "ok",
        "message": "Dashboard Movimentacao Estoque - Rede Hospital Casa",
        "endpoints": [
            "/api/overview", "/api/units", "/api/years",
            "/api/catalog", "/api/top50", "/api/item_units",
            "/api/item_kpis", "/api/item_detail", "/api/extrato",
            "/api/item_by_unit", "/api/loans"
        ]
    })


@app.route("/api/overview")
def overview():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        u = request.args.get("u", "")
        y = request.args.get("y", "")
        m = request.args.get("m", "")
        fe = []
        fs = []
        if u:
            fe.append(("unidade_norm", u))
            fs.append(("unidade_norm", u))
        if y:
            fe.append(("ano", y))
            fs.append(("ano", y))
        if m:
            fe.append(("mes", m))
            fs.append(("mes", m))
        qe = sb().table("entradas").select("ano, mes, unidade_norm, qtde, valor, tipo_mov")
        qe = apply_filters(qe, fe)
        ent_data = qe.limit(50000).execute().data
        qs = sb().table("saidas").select("ano, mes, unidade_norm, qtde, valor, tipo_mov")
        qs = apply_filters(qs, fs)
        sai_data = qs.limit(50000).execute().data
        monthly_e = {}
        for r in ent_data:
            key = (r["ano"] + "-" + r["mes"], r.get("unidade_norm", ""))
            if key not in monthly_e:
                monthly_e[key] = {"periodo": key[0], "unidade_norm": key[1], "eq": 0, "ev": 0}
            monthly_e[key]["eq"] += float(r.get("qtde") or 0)
            monthly_e[key]["ev"] += float(r.get("valor") or 0)
        monthly_s = {}
        for r in sai_data:
            key = (r["ano"] + "-" + r["mes"], r.get("unidade_norm", ""))
            if key not in monthly_s:
                monthly_s[key] = {"periodo": key[0], "unidade_norm": key[1], "sq": 0, "sv": 0}
            monthly_s[key]["sq"] += float(r.get("qtde") or 0)
            monthly_s[key]["sv"] += float(r.get("valor") or 0)
        et = {}
        for r in ent_data:
            tm = r.get("tipo_mov") or ""
            if tm not in et:
                et[tm] = [0, 0]
            et[tm][0] += float(r.get("qtde") or 0)
            et[tm][1] += float(r.get("valor") or 0)
        st = {}
        for r in sai_data:
            tm = r.get("tipo_mov") or ""
            if tm not in st:
                st[tm] = [0, 0]
            st[tm][0] += float(r.get("qtde") or 0)
            st[tm][1] += float(r.get("valor") or 0)
        return jsonify({
            "monthly_e": sorted(monthly_e.values(), key=lambda x: x["periodo"]),
            "monthly_s": sorted(monthly_s.values(), key=lambda x: x["periodo"]),
            "et": et, "st": st
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/units")
def units():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        r1 = sb().table("entradas").select("unidade_norm").limit(50000).execute().data
        r2 = sb().table("saidas").select("unidade_norm").limit(50000).execute().data
        all_units = sorted(set([r["unidade_norm"] for r in r1 if r.get("unidade_norm")] + [r["unidade_norm"] for r in r2 if r.get("unidade_norm")]))
        return jsonify(all_units)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/years")
def years():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        r1 = sb().table("entradas").select("ano").limit(50000).execute().data
        r2 = sb().table("saidas").select("ano").limit(50000).execute().data
        all_years = sorted(set([r["ano"] for r in r1 if r.get("ano")] + [r["ano"] for r in r2 if r.get("ano")]))
        return jsonify(all_years)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/catalog")
def catalog():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify([])
        r = sb().table("catalogo").select("cod_produto, desc_produto, freq").or_("cod_produto.like.%{0}%,desc_produto.ilike.%{0}%".format(q)).order("freq", desc=True).limit(30).execute()
        return jsonify([{"c": row["cod_produto"], "d": row["desc_produto"]} for row in r.data])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/top50")
def top50():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        ent_rows = sb().table("entradas").select("cod_produto, qtde, valor").limit(50000).execute().data
        sai_rows = sb().table("saidas").select("cod_produto, qtde, valor").limit(50000).execute().data
        ent = {}
        for r in ent_rows:
            c = r["cod_produto"]
            if c not in ent:
                ent[c] = [0, 0]
            ent[c][0] += float(r.get("qtde") or 0)
            ent[c][1] += float(r.get("valor") or 0)
        sai = {}
        for r in sai_rows:
            c = r["cod_produto"]
            if c not in sai:
                sai[c] = [0, 0]
            sai[c][0] += float(r.get("qtde") or 0)
            sai[c][1] += float(r.get("valor") or 0)
        all_codes = set(ent.keys()) | set(sai.keys())
        items = []
        for cod in all_codes:
            e = ent.get(cod, [0, 0])
            s = sai.get(cod, [0, 0])
            items.append({"c": cod, "eq": e[0], "ev": e[1], "sq": s[0], "sv": s[1], "t": e[0] + s[0]})
        items.sort(key=lambda x: -x["t"])
        top = items[:50]
        codes = [t["c"] for t in top]
        if codes:
            cat = sb().table("catalogo").select("cod_produto, desc_produto").in_("cod_produto", codes).execute().data
            desc_map = {}
            for r in cat:
                if r["cod_produto"] not in desc_map:
                    desc_map[r["cod_produto"]] = r["desc_produto"]
            for t in top:
                t["d"] = desc_map.get(t["c"], "")
        return jsonify(top)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/item_units")
def item_units():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        cod = request.args.get("cod", type=int)
        if not cod:
            return jsonify([])
        r1 = sb().table("entradas").select("unidade_norm").eq("cod_produto", cod).limit(50000).execute().data
        r2 = sb().table("saidas").select("unidade_norm").eq("cod_produto", cod).limit(50000).execute().data
        all_u = sorted(set([r["unidade_norm"] for r in r1 if r.get("unidade_norm")] + [r["unidade_norm"] for r in r2 if r.get("unidade_norm")]))
        return jsonify(all_u)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/item_kpis")
def item_kpis():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        cod = request.args.get("cod", type=int)
        if not cod:
            return jsonify({})
        u = request.args.get("u", "")
        y = request.args.get("y", "")
        m = request.args.get("m", "")
        fe = [("cod_produto", cod)]
        fs = [("cod_produto", cod)]
        if u:
            fe.append(("unidade_norm", u))
            fs.append(("unidade_norm", u))
        if y:
            fe.append(("ano", y))
            fs.append(("ano", y))
        if m:
            fe.append(("mes", m))
            fs.append(("mes", m))
        qe = sb().table("entradas").select("qtde, valor, ano, mes")
        qe = apply_filters(qe, fe)
        ent_data = qe.limit(50000).execute().data
        qs = sb().table("saidas").select("qtde, valor, ano, mes, tipo_mov")
        qs = apply_filters(qs, fs)
        sai_data = qs.limit(50000).execute().data
        eq = sum(float(r.get("qtde") or 0) for r in ent_data)
        ev = sum(float(r.get("valor") or 0) for r in ent_data)
        sq = sum(float(r.get("qtde") or 0) for r in sai_data)
        sv = sum(float(r.get("valor") or 0) for r in sai_data)
        acerto = sum(float(r.get("qtde") or 0) for r in sai_data if "ACERTO" in (r.get("tipo_mov") or "").upper() or "BAIXA" in (r.get("tipo_mov") or "").upper())
        ml_e = {}
        for r in ent_data:
            p = r["ano"] + "-" + r["mes"]
            if p not in ml_e:
                ml_e[p] = [p, 0, 0]
            ml_e[p][1] += float(r.get("qtde") or 0)
            ml_e[p][2] += float(r.get("valor") or 0)
        ml_s = {}
        for r in sai_data:
            p = r["ano"] + "-" + r["mes"]
            if p not in ml_s:
                ml_s[p] = [p, 0, 0]
            ml_s[p][1] += float(r.get("qtde") or 0)
            ml_s[p][2] += float(r.get("valor") or 0)
        return jsonify({"eq": eq, "ev": ev, "sq": sq, "sv": sv, "acerto": acerto, "ml_e": sorted(ml_e.values(), key=lambda x: x[0]), "ml_s": sorted(ml_s.values(), key=lambda x: x[0])})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/item_detail")
def item_detail():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        cod = request.args.get("cod", type=int)
        if not cod:
            return jsonify({})
        u = request.args.get("u", "")
        fe = [("cod_produto", cod)]
        fs = [("cod_produto", cod)]
        if u:
            fe.append(("unidade_norm", u))
            fs.append(("unidade_norm", u))
        qe = sb().table("entradas").select("tipo_mov, origem, qtde, valor")
        qe = apply_filters(qe, fe)
        ent_data = qe.limit(50000).execute().data
        qs = sb().table("saidas").select("tipo_mov, destino, qtde, valor")
        qs = apply_filters(qs, fs)
        sai_data = qs.limit(50000).execute().data
        ent_agg = {}
        for r in ent_data:
            key = (r.get("tipo_mov", ""), r.get("origem", ""))
            if key not in ent_agg:
                ent_agg[key] = [key[0], key[1], 0, 0]
            ent_agg[key][2] += float(r.get("qtde") or 0)
            ent_agg[key][3] += float(r.get("valor") or 0)
        sai_agg = {}
        for r in sai_data:
            key = (r.get("tipo_mov", ""), r.get("destino", ""))
            if key not in sai_agg:
                sai_agg[key] = [key[0], key[1], 0, 0]
            sai_agg[key][2] += float(r.get("qtde") or 0)
            sai_agg[key][3] += float(r.get("valor") or 0)
        return jsonify({"ent": sorted(ent_agg.values(), key=lambda x: (x[0], -x[2])), "sai": sorted(sai_agg.values(), key=lambda x: (x[0], -x[2]))})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/extrato")
def extrato():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        cod = request.args.get("cod", type=int)
        if not cod:
            return jsonify({"e": [], "s": []})
        u = request.args.get("u", "")
        y = request.args.get("y", "")
        fe = [("cod_produto", cod)]
        fs = [("cod_produto", cod)]
        if u:
            fe.append(("unidade_norm", u))
            fs.append(("unidade_norm", u))
        if y:
            fe.append(("ano", y))
            fs.append(("ano", y))
        qe = sb().table("entradas").select("dt, dt_sort, unidade_norm, tipo_mov, origem, qtde, valor")
        qe = apply_filters(qe, fe)
        ent_data = qe.order("dt_sort").limit(50000).execute().data
        qs = sb().table("saidas").select("dt, dt_sort, unidade_norm, tipo_mov, destino, qtde, valor")
        qs = apply_filters(qs, fs)
        sai_data = qs.order("dt_sort").limit(50000).execute().data
        ent_agg = {}
        for r in ent_data:
            key = (r["dt"], r.get("unidade_norm", ""), r.get("tipo_mov", ""), r.get("origem", ""))
            if key not in ent_agg:
                ent_agg[key] = [r["dt"], r.get("dt_sort", ""), key[1], key[2], key[3], 0, 0]
            ent_agg[key][5] += float(r.get("qtde") or 0)
            ent_agg[key][6] += float(r.get("valor") or 0)
        sai_agg = {}
        for r in sai_data:
            key = (r["dt"], r.get("unidade_norm", ""), r.get("tipo_mov", ""), r.get("destino", ""))
            if key not in sai_agg:
                sai_agg[key] = [r["dt"], r.get("dt_sort", ""), key[1], key[2], key[3], 0, 0]
            sai_agg[key][5] += float(r.get("qtde") or 0)
            sai_agg[key][6] += float(r.get("valor") or 0)
        return jsonify({"e": sorted(ent_agg.values(), key=lambda x: x[1]), "s": sorted(sai_agg.values(), key=lambda x: x[1])})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/item_by_unit")
def item_by_unit():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        cod = request.args.get("cod", type=int)
        if not cod:
            return jsonify([])
        ent = sb().table("entradas").select("unidade_norm, qtde").eq("cod_produto", cod).limit(50000).execute().data
        sai = sb().table("saidas").select("unidade_norm, qtde").eq("cod_produto", cod).limit(50000).execute().data
        data = {}
        for r in ent:
            u = r.get("unidade_norm", "")
            if u not in data:
                data[u] = [0, 0]
            data[u][0] += float(r.get("qtde") or 0)
        for r in sai:
            u = r.get("unidade_norm", "")
            if u not in data:
                data[u] = [0, 0]
            data[u][1] += float(r.get("qtde") or 0)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/loans")
def loans():
    if not sb():
        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
        cod = request.args.get("cod", type=int)
        if not cod:
            return jsonify([])
        ent = sb().table("entradas").select("origem, unidade_norm, qtde, valor").eq("cod_produto", cod).ilike("tipo_mov", "%EMPREST%").limit(50000).execute().data
        sai = sb().table("saidas").select("unidade_norm, destino, qtde, valor").eq("cod_produto", cod).ilike("tipo_mov", "%EMPREST%").limit(50000).execute().data
        loan_ent = {}
        for r in ent:
            key = (r.get("origem", "") or "") + " > " + (r.get("unidade_norm", "") or "")
            if key not in loan_ent:
                loan_ent[key] = [0, 0]
            loan_ent[key][0] += float(r.get("qtde") or 0)
            loan_ent[key][1] += float(r.get("valor") or 0)
        loan_sai = {}
        for r in sai:
            key = (r.get("unidade_norm", "") or "") + " > " + (r.get("destino", "") or "")
            if key not in loan_sai:
                loan_sai[key] = [0, 0]
            loan_sai[key][0] += float(r.get("qtde") or 0)
            loan_sai[key][1] += float(r.get("valor") or 0)
        all_keys = sorted(set(list(loan_ent.keys()) + list(loan_sai.keys())))
        result = []
        for key in all_keys:
            parts = key.split(" > ")
            s = loan_sai.get(key, [0, 0])
            e = loan_ent.get(key, [0, 0])
            result.append({"orig": parts[0], "dest": parts[1] if len(parts) > 1 else "", "sq": s[0], "sv": s[1], "eq": e[0], "ev": e[1], "diff": s[0] - e[0]})
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
