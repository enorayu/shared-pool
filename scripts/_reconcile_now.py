import urllib.request, json
REF="kgheakrpnpchtdtthoah"
SR="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtnaGVha3JwbnBjaHRkdHRob2FoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4NDg2MTkwOCwiZXhwIjoyMTAwNDM3OTA4fQ.vThMMA1ICwgKsAcIPxffpqEDmKoaUmNJZdOmtD_Yk6o"
BASE="https://%s.supabase.co/rest/v1"%REF

def cnt(table, flt):
    url=BASE+"/%s?select=*%s"%(table,flt)
    req=urllib.request.Request(url, headers={"apikey":SR,"Authorization":"Bearer %s"%SR,"Prefer":"count=exact"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.headers.get("content-range","?")
    except Exception as e:
        return "ERR:"+str(e)[:160]

print("price_pool total:", cnt("price_pool",""))
print("price_pool normalized==meup (both not null):",
      cnt("price_pool","&and=(normalized_price.not.is.null,meup_price.not.is.null,normalized_price.eq.meup_price)"))
print("quote_pool total:", cnt("quote_pool",""))
# quote_pool: supplier 空 且 price==meup 数值 —— 用 and 多条件
print("quote_pool supplier is null & price==meup (both not null):",
      cnt("quote_pool","&and=(supplier.is.null,price.not.is.null,meup_price.not.is.null,price.eq.meup_price)"))
