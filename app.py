import os
from flask import Flask, jsonify, request
from supabase import create_client, Client

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

if SUPABASE_URL and SUPABASE_KEY:
          supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
else:
          supabase = None


@app.route("/")
def index():
          return jsonify({
                        "status": "ok",
                        "message": "Dashboard Itens de Consumo API",
                        "endpoints": ["/api/itens", "/api/resumo", "/api/grupos"]
          })


@app.route("/api/itens")
def listar_itens():
          if not supabase:
                        return jsonify({"error": "Supabase nao configurado"}), 500
                    try:
                                  ano = request.args.get("ano")
                                  mes = request.args.get("mes")
                                  grupo = request.args.get("grupo")
                                  query = supabase.table("itens_consumo").select("*")
                                  if ano:
                                                    query = query.eq("ano", int(ano))
                                                if mes:
                                                                  query = query.eq("mes", int(mes))
                                                              if grupo:
                                                                                query = query.eq("grupo", grupo)
                                                                            response = query.order("created_at", desc=True).limit(500).execute()
        return jsonify({"data": response.data, "count": len(response.data)})
except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/resumo")
def resumo():
          if not supabase:
                        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
                  response = supabase.table("itens_consumo").select(
                                    "grupo, valor_total, quantidade"
                  ).execute()
        grupos = {}
        for item in response.data:
                          g = item.get("grupo") or "Sem grupo"
            if g not in grupos:
                                  grupos[g] = {"grupo": g, "total_valor": 0, "count": 0}
            grupos[g]["total_valor"] += float(item.get("valor_total") or 0)
            grupos[g]["count"] += 1
        return jsonify({"data": list(grupos.values())})
except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/grupos")
def listar_grupos():
          if not supabase:
                        return jsonify({"error": "Supabase nao configurado"}), 500
    try:
                  response = supabase.table("grupos").select("*").execute()
        return jsonify({"data": response.data})
except Exception as e:
        return jsonify({"error": str(e)}), 500
