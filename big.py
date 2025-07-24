import os
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime
from flask import Flask, request, jsonify
from typing import Tuple, List, Dict, Optional

# Load environment variables
load_dotenv('acc.env')

class BigQueryBillManager:
    def __init__(self):
        self.client = bigquery.Client()
        self.table_id = f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE')}"
    
    def _generate_record_id(self) -> str:
        """Generate unique ID based on current timestamp"""
        return datetime.now().strftime("%Y%m%d%H%M%S%f")
    
    def insert_bill(self, bill_text: str, user_prompt: str, ai_response: str) -> Tuple[bool, str]:
        """Insert a bill record into BigQuery"""
        try:
            record = {
                "record_id": self._generate_record_id(),
                "bill_date": datetime.now().isoformat(),
                "bill_details": bill_text,
                "user_prompt": user_prompt,
                "ai_response": ai_response
            }
            
            errors = self.client.insert_rows_json(self.table_id, [record])
            return (True, "Success") if not errors else (False, str(errors))
        
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
    
    def get_bills(self, days: int = 7, limit: Optional[int] = None) -> List[Dict]:
        """Retrieve bills from last N days"""
        try:
            query = f"""
                SELECT * FROM `{self.table_id}`
                WHERE DATE(bill_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
                ORDER BY created_at DESC
                {f'LIMIT {limit}' if limit else ''}
            """
            return [dict(row) for row in self.client.query(query).result()]
        
        except Exception as e:
            print(f"Query failed: {str(e)}")
            return []
    
    def search_bills(self, search_term: str, days: int = 30) -> List[Dict]:
        """Search bills containing specific text"""
        try:
            query = f"""
                SELECT * FROM `{self.table_id}`
                WHERE DATE(bill_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
                AND bill_details LIKE '%{search_term}%'
                ORDER BY created_at DESC
            """
            return [dict(row) for row in self.client.query(query).result()]
        
        except Exception as e:
            print(f"Search failed: {str(e)}")
            return []

# Flask API Setup
app = Flask(__name__)
bill_manager = BigQueryBillManager()

@app.route('/api/bills', methods=['POST'])
def add_bill():
    """Endpoint to add new bills"""
    data = request.json
    if not all(k in data for k in ['bill_text', 'user_prompt', 'ai_response']):
        return jsonify({"status": "error", "message": "Missing required fields"}), 400
    
    success, message = bill_manager.insert_bill(
        bill_text=data['bill_text'],
        user_prompt=data['user_prompt'],
        ai_response=data['ai_response']
    )
    
    if success:
        return jsonify({"status": "success"}), 201
    else:
        return jsonify({"status": "error", "message": message}), 500

@app.route('/api/bills', methods=['GET'])
def get_bills():
    """Endpoint to retrieve bills"""
    days = int(request.args.get('days', 7))
    limit = int(request.args.get('limit', 0)) or None
    bills = bill_manager.get_bills(days=days, limit=limit)
    return jsonify({"status": "success", "data": bills})

@app.route('/api/bills/search', methods=['GET'])
def search_bills():
    """Endpoint to search bills"""
    search_term = request.args.get('q')
    if not search_term:
        return jsonify({"status": "error", "message": "Missing search term"}), 400
    
    days = int(request.args.get('days', 30))
    results = bill_manager.search_bills(search_term=search_term, days=days)
    return jsonify({"status": "success", "data": results})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)