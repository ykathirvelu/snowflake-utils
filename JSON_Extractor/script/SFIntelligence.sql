CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE PUBLIC;

CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;
GRANT USAGE ON SCHEMA snowflake_intelligence.agents TO ROLE PUBLIC;

GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE accountadmin;
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE sysadmin;
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE workshopadmin;

CREATE OR REPLACE STAGE LEGACY_CODE_STAGE;
CREATE OR REPLACE STAGE CONVERTED_CODE_STAGE;

-----------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE snowflake_intelligence;
USE SCHEMA public;

CREATE OR REPLACE NETWORK RULE openai_host_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.openai.com');

CREATE OR REPLACE SECRET openai_key
  TYPE = GENERIC_STRING
  SECRET_STRING = '<>'  ;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION openai_eai
  ALLOWED_NETWORK_RULES = (openai_host_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (openai_key)
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION openai_eai TO ROLE accountadmin;
GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE accountadmin;
GRANT USAGE ON SCHEMA snowflake_intelligence.public TO ROLE accountadmin;
GRANT USAGE ON SECRET openai_key TO ROLE accountadmin;


CREATE OR REPLACE TABLE ai_agent_history (
    conv_id STRING,
    ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    role STRING,         -- 'system' | 'user' | 'assistant'
    content STRING
);

CREATE OR REPLACE PROCEDURE ai_code_agent(
    CONV_ID STRING,
    USER_INPUT STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (openai_eai)
SECRETS = ('openai_api_key' = openai_key)
AS
$$
import json, requests, _snowflake

OPENAI_URL = "https://api.openai.com/v1/chat/completions"
MODEL = "gpt-4o-mini"

def get_history(session, conv_id):
    rows = session.sql(f"SELECT role, content FROM ai_agent_history WHERE conv_id = '{conv_id}' ORDER BY ts").collect()
    return [{"role": r["ROLE"], "content": r["CONTENT"]} for r in rows]

def save_message(session, conv_id, role, content):
    session.sql("INSERT INTO ai_agent_history (conv_id, role, content) VALUES (?, ?, ?)", 
                (conv_id, role, content)).collect()

def execute_sql(session, sql_text: str):
    try:
        df = session.sql(sql_text).collect()
        return f"Executed successfully. Returned {len(df)} rows. Example: {df[:3]}"
    except Exception as e:
        return f"Execution error: {str(e)}"

def run(session, CONV_ID: str, USER_INPUT: str):
    api_key = _snowflake.get_generic_secret_string('openai_api_key')

    # Load conversation
    history = get_history(session, CONV_ID)

    # First turn? Add system prompt
    if not history:
        system_prompt = (
            "You are a Snowflake AI Agent. "
            "You can convert code, explain it, or execute SQL queries directly in Snowflake. "
            "If the user asks you to run something, decide whether to call the `execute_sql` tool. "
            "When you call a tool, output only a JSON object like {\"tool\": \"execute_sql\", \"input\": \"...\"}. "
            "Otherwise, reply normally."
        )
        save_message(session, CONV_ID, "system", system_prompt)
        history = [{"role":"system","content":system_prompt}]

    # Add user message
    save_message(session, CONV_ID, "user", USER_INPUT)
    history.append({"role":"user","content":USER_INPUT})

    # Call OpenAI
    payload = {"model": MODEL, "messages": history, "temperature": 0.3, "max_tokens": 1500}
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = requests.post(OPENAI_URL, headers=headers, data=json.dumps(payload), timeout=60)
    if resp.status_code != 200:
        raise Exception(f"OpenAI error {resp.status_code}: {resp.text}")

    reply = resp.json()["choices"][0]["message"]["content"]

    # Did the model request a tool?
    if reply.strip().startswith("{") and '"tool":' in reply:
        try:
            tool_call = json.loads(reply)
            tool = tool_call.get("tool")
            tool_input = tool_call.get("input","")
        except Exception:
            return reply  # badly formatted, just return

        if tool == "execute_sql":
            tool_result = execute_sql(session, tool_input)
            save_message(session, CONV_ID, "tool", f"execute_sql result: {tool_result}")
            # Re-ask OpenAI with tool output
            history.append({"role":"tool","content":f"execute_sql result: {tool_result}"})
            payload = {"model": MODEL, "messages": history, "temperature": 0.3}
            resp = requests.post(OPENAI_URL, headers=headers, data=json.dumps(payload), timeout=60)
            if resp.status_code != 200:
                raise Exception(f"OpenAI error {resp.status_code}: {resp.text}")
            reply = resp.json()["choices"][0]["message"]["content"]

    # Save assistant reply
    save_message(session, CONV_ID, "assistant", reply)
    return reply
$$;

-------------------

USE ROLE ACCOUNTADMIN;

CREATE ROLE copilot_access_role;
GRANT DATABASE ROLE SNOWFLAKE.COPILOT_USER TO ROLE copilot_access_role;

GRANT ROLE copilot_access_role TO USER sfadmin;


CREATE TASK yk_t1
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AFTER my_yk_task
  AS
    EXECUTE IMMEDIATE
    $$
    DECLARE
      radius_of_circle float;
      area_of_circle float;
    BEGIN
      radius_of_circle := 3;
      area_of_circle := pi() * radius_of_circle * radius_of_circle;
      return area_of_circle;
    END;
    $$;


CREATE TASK my_yk_task
    WAREHOUSE = compute_wh
    WHEN SYSTEM$STREAM_HAS_DATA('strm_movilens_nation')
    AS
      SELECT CURRENT_TIMESTAMP;

CREATE TASK my_yk_task_a
    WAREHOUSE = compute_wh
    after yk_t1
    WHEN SYSTEM$GET_PREDECESSOR_RETURN_VALUE('yk_t1') > 100.0
    AS
      SELECT CURRENT_TIMESTAMP;      