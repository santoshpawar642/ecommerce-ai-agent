import boto3
import json

# --- 1. Configuration ---
# Your region from the screenshot was Europe (Stockholm)
AWS_REGION = "eu-north-1" 

# This is the EXACT ID from your 'aws bedrock list-inference-profiles' output
# Claude 4.5 Haiku is the current active model in 2026.
ACTIVE_MODEL_ID = "eu.anthropic.claude-haiku-4-5-20251001-v1:0"

def test_brain_connection():
    """
    This function tests if your MacBook can successfully send a prompt
    to the AWS Bedrock 'Brain' and receive a SQL response.
    """
    # Initialize the Bedrock Runtime client
    client = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    # We are asking a specific SQL question to test its 'Data Analyst' skills
    user_prompt = "Write a Snowflake SQL query to find the total revenue for the city 'Pune' from a table named ECOM_DB.GOLD.FACT_SALES."

    print(f"--- CONNECTING TO BEDROCK ({ACTIVE_MODEL_ID}) ---")
    
    try:
        # Using the Converse API (The production standard for 2026)
        response = client.converse(
            modelId=ACTIVE_MODEL_ID,
            messages=[
                {
                    "role": "user", 
                    "content": [{"text": user_prompt}]
                }
            ],
            inferenceConfig={
                "maxTokens": 512,
                "temperature": 0.1  # Low temperature = more accurate SQL
            }
        )

        # Extract the text from the response object
        ai_answer = response['output']['message']['content'][0]['text']
        
        print("\n[SUCCESS] The Brain Responded:")
        print("-" * 30)
        print(ai_answer)
        print("-" * 30)
        print("\nYour MacBook is now an AI-powered workstation. Ready for the final Agent!")

    except Exception as e:
        print(f"\n[ERROR] Connection failed: {str(e)}")
        print("\n--- TROUBLESHOOTING ---")
        if "AccessDeniedException" in str(e):
            print("1. Go to IAM in AWS Console and ensure your user has 'AmazonBedrockFullAccess'.")
            print("2. Run 'aws configure' in terminal to refresh your keys.")
        elif "ValidationException" in str(e):
            print("1. Check Bedrock > Model Access to see if 'Claude Haiku 4.5' is 'Access Granted'.")
            print("2. If it's not granted, click 'Submit Use Case Details' for that specific model.")

if __name__ == "__main__":
    test_brain_connection()