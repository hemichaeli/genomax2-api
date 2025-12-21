"""
Debug endpoint for checking catalog metadata.
Import this in api_server.py to add the endpoint.
"""

def register_debug_endpoints(app, get_db):
    """Register debug endpoints on the FastAPI app."""
    
    @app.get("/debug/catalog-check")
    def check_catalog():
        """Debug endpoint to check catalog and ingredients metadata."""
        conn = get_db()
        if not conn:
            return {"error": "DB connection failed"}
        try:
            cur = conn.cursor()
            
            # Check fulfillment_catalog structure
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'fulfillment_catalog'
                ORDER BY ordinal_position
            """)
            catalog_columns = [{"name": row["column_name"], "type": row["data_type"]} for row in cur.fetchall()]
            
            # Count products
            cur.execute("SELECT COUNT(*) as total FROM fulfillment_catalog")
            total_products = cur.fetchone()["total"]
            
            # Check ingredients table structure  
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'ingredients'
                ORDER BY ordinal_position
            """)
            ingredient_columns = [{"name": row["column_name"], "type": row["data_type"]} for row in cur.fetchall()]
            
            # Count ingredients with metadata
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN category IS NOT NULL AND length(trim(category)) > 0 THEN 1 ELSE 0 END) as with_category,
                    SUM(CASE WHEN contraindications IS NOT NULL AND length(trim(contraindications)) > 0 THEN 1 ELSE 0 END) as with_contraindications
                FROM ingredients
            """)
            ing_stats = cur.fetchone()
            
            # Sample products with ingredient info
            cur.execute("""
                SELECT 
                    fc.id, fc.product_name, fc.supplier_name,
                    i.name as ingredient_name, i.category, i.contraindications
                FROM fulfillment_catalog fc
                LEFT JOIN ingredients i ON fc.ingredient_id = i.id
                LIMIT 5
            """)
            samples = [dict(row) for row in cur.fetchall()]
            
            # Get distinct categories
            cur.execute("SELECT DISTINCT category FROM ingredients WHERE category IS NOT NULL ORDER BY category")
            categories = [row["category"] for row in cur.fetchall()]
            
            # Products without ingredient link
            cur.execute("SELECT COUNT(*) as count FROM fulfillment_catalog WHERE ingredient_id IS NULL")
            unlinked = cur.fetchone()["count"]
            
            cur.close()
            conn.close()
            
            return {
                "fulfillment_catalog": {
                    "columns": catalog_columns,
                    "total_products": total_products,
                    "unlinked_products": unlinked
                },
                "ingredients": {
                    "columns": ingredient_columns,
                    "total": ing_stats["total"],
                    "with_category": ing_stats["with_category"],
                    "with_contraindications": ing_stats["with_contraindications"],
                    "categories": categories
                },
                "sample_products": samples,
                "routing_readiness": {
                    "can_filter_by_category": ing_stats["with_category"] > 0,
                    "can_filter_by_contraindications": ing_stats["with_contraindications"] > 0,
                    "coverage_percent": round((ing_stats["with_category"] / ing_stats["total"]) * 100, 1) if ing_stats["total"] > 0 else 0
                }
            }
        except Exception as e:
            try:
                conn.close()
            except:
                pass
            return {"error": str(e)}
