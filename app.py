from flask import Flask, request, jsonify
from databricks.sql import connect
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configuration
CATALOG = "dws_db"
SCHEMA = "dws_genai"
TABLE = "final_df_ques_schema_delta"

# Databricks connection parameters
DATABRICKS_SERVER_HOSTNAME = os.environ.get("DATABRICKS_HOST", "adb-3160704799700472.12.azuredatabricks.net")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/a82ac8a0e49ed245")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "dapiaab7d788ba46cc8e498b7ac7b6113002-3")

def get_connection():
    """Create and return a connection to Databricks SQL"""
    return connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

@app.route('/get_questions', methods=['GET'])
def get_questions():
    """Fetch all questions from the Databricks table"""
    try:
        # Connect to Databricks
        with get_connection() as connection:
            with connection.cursor() as cursor:
                # Query to get all rows
                query = f"""
                    SELECT Question_id, derived_column_ID, derived_column, questions, Type_of_Questions,
                           Right_question, Wrong_question, wrong_context
                    FROM {CATALOG}.{SCHEMA}.{TABLE}
                """
                cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Convert result to list of dictionaries
                questions = []
                for row in cursor.fetchall():
                    question_dict = dict(zip(columns, row))
                    questions.append({
                        "ques_id": question_dict["Question_id"],
                        "id": question_dict["derived_column_ID"],
                        "derived_column": question_dict["derived_column"],
                        "question": question_dict["questions"],
                        "type": question_dict["Type_of_Questions"],
                        "right_question": question_dict["Right_question"],
                        "wrong_question": question_dict["Wrong_question"],
                        "wrong_context": question_dict["wrong_context"]
                    })
                
                return jsonify(questions)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/submit_feedback', methods=['POST'])
def submit_feedback():
    """Update feedback for a specific question"""
    data = request.json
    print("Received Data:", data)  # Debugging

    if not data or 'ques_id' not in data:
        return jsonify({"error": "Missing question ID"}), 400
    
    # Validate that only one feedback value is 1 and others are 0
    feedback_values = (data.get("Right_question", 0), data.get("Wrong_question", 0), data.get("wrong_context", 0))
    
    if feedback_values not in [(1, 0, 0), (0, 1, 0), (0, 0, 1)]:
        return jsonify({"error": "Invalid input: Only one of Right_question, Wrong_question, or wrong_context can be 1."}), 400

    # Prepare update query
    updates = [f"{key} = {value}" for key, value in zip(["Right_question", "Wrong_question", "wrong_context"], feedback_values)]

    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                update_sql = f"""
                UPDATE {CATALOG}.{SCHEMA}.{TABLE}
                SET {', '.join(updates)}
                WHERE Question_id = '{data['ques_id']}'
                """
                cursor.execute(update_sql)

                return jsonify({
                    "status": "success",
                    "message": f"Updated feedback for question ID {data['ques_id']}"
                })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run()