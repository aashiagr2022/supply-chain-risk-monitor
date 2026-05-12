import os
import sys
from openai import OpenAI
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = """You are an expert supply chain risk analyst working for a 
manufacturing company. You analyze supplier performance data and provide 
specific, actionable recommendations to prevent production disruptions.

Your analysis must be:
- Specific to the supplier and situation described
- Data-backed — reference the actual numbers provided
- Actionable — give concrete steps the procurement team can take today
- Concise — maximum 4-5 sentences

Always structure your response as:
1. Root cause (1 sentence)
2. Risk level and why (1 sentence) 
3. Immediate action (1-2 sentences)
4. Longer term recommendation (1 sentence)"""

def analyze_supplier_risk(
    supplier_name: str,
    anomaly_description: str,
    severity: str,
    rag_context: str
) -> str:
    """
    Calls OpenAI with RAG-enriched context to generate
    a specific actionable risk assessment.
    """

    user_prompt = f"""
Analyze this supply chain risk situation and provide specific recommendations:

ANOMALY DETECTED:
- Supplier: {supplier_name}
- Severity: {severity}
- Issues: {anomaly_description}

CONTEXT FROM OUR DATA:
{rag_context}

Provide a specific, actionable risk assessment based on all the above data.
Reference actual numbers and name specific backup suppliers if available.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=300,
            temperature=0.3
        )

        analysis = response.choices[0].message.content.strip()
        print(f"  🤖 AI analysis generated for {supplier_name}")
        return analysis

    except Exception as e:
        print(f"  ⚠️  OpenAI error: {e}")
        return f"AI analysis unavailable: {str(e)}"


def text_to_sql(natural_language_query: str, schema_context: str) -> str:
    """
    Converts a natural language question into a SQL query.
    This powers the natural language query endpoint in FastAPI.
    """

    prompt = f"""Convert this natural language question into a PostgreSQL SQL query.

DATABASE SCHEMA:
{schema_context}

QUESTION: {natural_language_query}

Rules:
- Return ONLY the SQL query, nothing else
- No markdown, no explanation, no backticks
- Use proper PostgreSQL syntax
- Limit results to 20 rows maximum
- Only use SELECT statements — no INSERT, UPDATE, DELETE
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": prompt}
            ],
            max_tokens=200,
            temperature=0
        )

        sql = response.choices[0].message.content.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()
        return sql

    except Exception as e:
        return f"Error generating SQL: {str(e)}"