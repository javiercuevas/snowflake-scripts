import pandas as pd

def main(session, email_integration_name, email_address):
  df = session.table("MY_TABLE").to_pandas()
  html_content = df.to_markdown(tablefmt="html", index=False)
  session.call("SYSTEM$SEND_EMAIL", email_integration_name, email_address, "Query Results", html_content, "text/html")
  return "Email sent successfully"
