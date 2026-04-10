import os
from flask import Flask, jsonify, request
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


@app.route("/")
def index():
      return "Dashboard Itens de Consumo - API rodando"


@app.route("/api/itens")
def listar_itens():
      try:
                response = supabase.table("itens_consumo").select("*").limit(500).execute()
                return jsonify({"data": response.data, "count": len(response.data)})
except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/resumo")
def resumo():
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


if __name__ == "__main__":
      app.run(debug=True, port=5000)
