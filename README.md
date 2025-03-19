
# Setup Instructions

## Step 1: Install Required Libraries
Run the following command to install the necessary libraries from `requirements.txt`:
```bash
pip install -r requirements.txt
```

## Step 2: Generate OpenAI API Key
1. Go to [OpenAI API Keys](https://platform.openai.com/signup) and sign up or log in.
2. Generate your API key.

## Step 3: Import Data into SQL Database
1. Download the dataset from the following GitHub repository:  
   [OLTP-AdventureWorks2019-MySQL](https://github.com/vishal180618/OLTP-AdventureWorks2019-MySQL)  
   *Credit to [Vishal](https://github.com/vishal180618) for providing the database.*
2. Import the `.sql` file into your MySQL database.

## Step 4: Create a `.env` File
Create a `.env` file in the root directory and add the following information:

```
OPENAI_API_KEY=your_openai_api_key
MYSQL_HOST=your_mysql_host
MYSQL_PORT=your_mysql_port
MYSQL_USER=your_mysql_username
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=your_mysql_database_name
```

## Step 5: Run the Application
Run the following command to execute the script:

```bash
python all_in_one.py
```

**Note:** Ensure the `.env` file is included in your `.gitignore` to avoid exposing sensitive information.
