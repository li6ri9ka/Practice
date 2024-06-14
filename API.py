from flask import Flask, request, jsonify
import psycopg2
import config

app = Flask(__name__)

class DatabaseManager:
    def __init__(self, db_info):
        self.db_info = db_info
        self.connection = None

    def connect(self):
        if not self.connection:
            self.connection = psycopg2.connect(**self.db_info)
        return self.connection

    def fetch_logs(self, ip=None, start_date=None, end_date=None, group_by=None):
        conn = self.connect()
        cursor = conn.cursor()
        query = 'SELECT * FROM logs WHERE TRUE'
        if ip:
            query += f" AND server_ip = '{ip}'"
        if start_date:
            query += f" AND date_time >= '{start_date}'"
        if end_date:
            query += f" AND date_time <= '{end_date}'"
        if group_by:
            query += f" GROUP BY {group_by}"
        cursor.execute(query)
        logs = cursor.fetchall()
        cursor.close()
        return logs

@app.route('/logs', methods=['GET'])
def get_logs():
    db_manager = DatabaseManager(config.db_info)
    ip = request.args.get('ip', default=None)
    start_date = request.args.get('start_date', default=None)
    end_date = request.args.get('end_date', default=None)
    group_by = request.args.get('group_by', default=None)

    logs = db_manager.fetch_logs(ip, start_date, end_date, group_by)
    columns = ['ip', 'timestamp', 'method', 'url', 'status', 'user_agent']
    json_logs = [dict(zip(columns, log)) for log in logs]

    return jsonify(json_logs)

if __name__ == '__main__':
    app.run(debug=True)
