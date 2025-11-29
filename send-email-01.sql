--https://medium.com/@akshayrs1993/sending-email-alerts-from-%EF%B8%8Fsnowflake-stored-procedures-tasks-on-errors-53a2db63de43?postPublishedType=repub

CREATE OR REPLACE PROCEDURE send_email_on_error() 
RETURNS STRING 
LANGUAGE SQL 
EXECUTE AS CALLER AS 
$$ 
DECLARE 
error_message STRING; 
BEGIN   
 
select 10 / 0;   -- Simulate an error (division by zero)

EXCEPTION     
  WHEN OTHER THEN       -- Capture the error message       
    error_message := SQLERRM;   
               
    -- Send email notification       
    CALL SYSTEM$SEND_EMAIL(         
    'my_email_integration',-- Name of the email integration         
    'your.email@example.com', -- Recipient email         
    '🚨🚨 Snowflake Error Alert: Task/Procedure has failed 🚨🚨', -- Subject         
    'An error occurred: ' || error_message -- Body 
    );              
    
    -- Return a message to the caller       
    RETURN 'Error caught and email sent: ' || error_message;   

-- If no error, return success   
RETURN 'Procedure completed successfully';

END; 
$$;
