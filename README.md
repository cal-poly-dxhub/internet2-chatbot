# Internet2 Chatbot


## Table of Contents
- [Collaboration](#collaboration)
- [Disclaimers](#disclaimers)
- [Overview](#chatbot-overview)
- [Deployment Steps](#deployment-steps)



# Collaboration
Thanks for your interest in our solution.  Having specific examples of replication and cloning allows us to continue to grow and scale our work. If you clone or download this repository, kindly shoot us a quick email to let us know you are interested in this work!

[wwps-cic@amazon.com]

# Disclaimers

**Customers are responsible for making their own independent assessment of the information in this document.**

**This document:**

(a) is for informational purposes only,

(b) represents current AWS product offerings and practices, which are subject to change without notice, and

(c) does not create any commitments or assurances from AWS and its affiliates, suppliers or licensors. AWS products or services are provided “as is” without warranties, representations, or conditions of any kind, whether express or implied. The responsibilities and liabilities of AWS to its customers are controlled by AWS agreements, and this document is not part of, nor does it modify, any agreement between AWS and its customers.

(d) is not to be considered a recommendation or viewpoint of AWS

**Additionally, all prototype code and associated assets should be considered:**

(a) as-is and without warranties

(b) not suitable for production environments

(d) to include shortcuts in order to support rapid prototyping such as, but not limitted to, relaxed authentication and authorization and a lack of strict adherence to security best practices

**All work produced is open source. More information can be found in the GitHub repo.**

## Chatbot Overview
- The [DxHub](https://dxhub.calpoly.edu/challenges/) developed a chatbot solution that can answer user questions pulling from their knowledge base articles. The chatbot contains many features:

    #### Intelligent Question Answering
    - Leverages Retrieval Augmented Generation (RAG) for accurate and contextual responses
    - Dynamic context integration for more relevant and precise answers
    - Real-time information retrieval

    #### Source Attribution
    - Direct links to source documents
    - Easy access to reference materials

    #### Scalability and Versitility
    - Serverless architecture enables automatic scaling
    - API-first design supports multiple frontend implementations

## Deployment Steps

### Prerequisites
- AWS CDK CLI, Docker (running), Python 3.x, Git, a CDK Bootstrapped environment
- AWS credentials configured

### Step 1: Clone & Setup
```bash
git clone https://github.com/cal-poly-dxhub/internet2-chatbot.git
cd internet2-chatbot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mv example_config.yaml config.yaml
```

### Step 2: Request Bedrock Model Access
In AWS Bedrock console → Model access, request access for:
- `anthropic.claude-3-5-sonnet-20241022-v2:0`
- `amazon.titan-embed-text-v2:0`
- `anthropic.claude-3-haiku-20240307-v1:0`

### Step 3: Deploy Infrastructure
```bash
cdk deploy
```

### Step 4: Update config.yaml
Update with CDK outputs:
```yaml
opensearch_endpoint: <FROM_CDK_OUTPUT>
rag_api_endpoint: <FROM_CDK_OUTPUT>
s3_bucket_name: <FROM_CDK_OUTPUT>
step_function_arn: <FROM_CDK_OUTPUT>
processed_files_table: <FROM_CDK_OUTPUT>
api_key: <FROM_API_GATEWAY_CONSOLE>
```

### Step 5: Upload Documents & Run Processing

#### Option A: Manual File Upload
```bash
# Upload files to S3
aws s3 cp your-documents/ s3://<S3_BUCKET_NAME>/files-to-process/ --recursive
```

#### Option B: Google Drive Integration
**Prerequisites**: Google Cloud account, Atlassian account, LibreOffice installed

**1. Set Up Atlassian API Access:**
- Go to https://id.atlassian.com/manage-profile/security/api-tokens
- Click "Create API token", label it, and copy the token
- Save this token for later use

**2. Set Up Google Service Account:**
- Go to [Google Cloud Console](https://console.cloud.google.com/)
- Create a new project (or use existing)
- Go to "APIs & Services" > "Library" and enable "Google Drive API"
- Go to "APIs & Services" > "Credentials"
- Click "Create Credentials" > "Service account"
- Give it a name and click "Create and Continue" > "Done"
- Click your new service account > "Keys" tab > "Add Key" > "Create new key" > "JSON"
- Download and save the JSON file

**3. Share Google Drive Access:**
- Open Google Drive and right-click folders/files you want to ingest
- Click "Share" and add the service account email (from JSON file: `xxxx@xxxx.iam.gserviceaccount.com`)
- Set as "Viewer" and click "Send"

**4. Install LibreOffice:**
```bash
# macOS
brew install --cask libreoffice

# Ubuntu/Debian
sudo apt-get install libreoffice
```

**5. Configure Environment:**
```bash
# Create and edit names.env file
cp names.env.example names.env

# Set these variables in names.env:
SERVICE_ACC_SECRET_NAME=default-service-account-name
GOOGLE_DRIVE_CREDENTIALS=/path/to/your/service-account.json
GOOGLE_API_KEY=your-google-api-key
CONFLUENCE_API=your_atlassian_api_token_from_step_1
# Load environment variables
source names.env

# Set these variables in config.yaml and check for any missing fields
CONFLUENCE_URL=your-confleunce-url-to-scrape
```

**6. Run Ingestion:**
```bash
# Scrape asset links from Confluence wiki (creates a .csv file with links to folders)
python confluence_processor.py

# Download from Google Drive and upload to S3
python google_drive_processor_enhanced.py

# Download .txt files from the wiki page descriptions section and upload to S3
python confluence_event_descriptions_to_s3.py
```

### Step 6: Run Document Processing
```bash
cd ingest_utils
python3 run_step_function.py
```
This automatically creates the OpenSearch index if needed, then starts document processing.

**Note:** Files set for processing are saved to DynamoDB to ensure there are no lost updates due to concurrent operations. To reset this cache run:

```bash
python3 run_step_function.py --reset-cache
```

### Step 7: Test (Can Start Immediately)
```bash
python3 chat_test.py
```
**Note**: You can start testing immediately after Step 6, but response quality will improve as more documents are processed. Wait for full ingestion completion for best results.

## Troubleshooting
- **Docker access**: `sudo usermod -aG docker $USER && newgrp docker`
- **CDK issues**: Check `aws sts get-caller-identity` and run `cdk bootstrap`
- **Model access**: Verify in Bedrock console
- **Processing fails**: Check Step Function logs in AWS Console
- **Chat issues**: Verify API key and endpoint accessibility

## Known Issues
- Quick PoC with no intent verification or error checking

## Support
For queries or issues:
- Darren Kraker, Sr Solutions Architect - dkraker@amazon.com
- Nick Riley, Jr SDE - njriley@calpoly.edu
- Kartik Malunjkar, Software Development Engineer Intern- kmalunjk@calpoly.edu
