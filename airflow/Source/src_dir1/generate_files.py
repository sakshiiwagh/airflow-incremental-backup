import os
import random
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the source directories relative to the script's location
dir1 = os.path.join(script_dir, "src_dir1")
dir2 = os.path.join(script_dir, "src_dir2")

# Create directories if they don't exist
os.makedirs(dir1, exist_ok=True)
os.makedirs(dir2, exist_ok=True)
logging.info(f"Directories created or already exist:\n{dir1}\n{dir2}")

# Define the file types and extensions
file_types = {
    "text": ".txt",
    "csv": ".csv",
    "json": ".json",
    "xml": ".xml",
    "html": ".html",
    "log": ".log",
    "python": ".py",
    "markdown": ".md",
    "config": ".cfg",
    "ini": ".ini",
    "javascript": ".js",
    "java": ".java",
    "csharp": ".cs",
    "cpp": ".cpp",
    "c": ".c",
    "bash": ".sh",
    "powershell": ".ps1",
    "yaml": ".yaml",
    "toml": ".toml",
    "sql": ".sql",
    "data": ".dat",
    "binary": ".bin",
    "image": ".png",  # Placeholder image extension
    "video": ".mp4",  # Placeholder video extension
    "audio": ".mp3",  # Placeholder audio extension
    "pdf": ".pdf",
}

# Sample content for each file type
sample_content = {
    "text": "This is a sample text file.",
    "csv": "col1,col2,col3\n1,2,3\n4,5,6",
    "json": '{"key": "value", "number": 123}',
    "xml": "<root><element>Sample</element></root>",
    "html": "<html><body><h1>Sample HTML</h1></body></html>",
    "log": "INFO: This is a sample log file.\nERROR: An example error.",
    "python": "# This is a sample Python script\nprint('Hello, World!')",
    "markdown": "# Sample Markdown File\n- Item 1\n- Item 2",
    "config": "[Section]\nkey=value",
    "ini": "[Settings]\noption=enabled",
    "javascript": "// Sample JavaScript\nconsole.log('Hello, World!');",
    "java": "// Sample Java Code\npublic class Main { public static void main(String[] args) { System.out.println('Hello, World!'); } }",
    "csharp": "// Sample C# Code\nclass Program { static void Main() { System.Console.WriteLine('Hello, World!'); } }",
    "cpp": "// Sample C++ Code\n#include <iostream>\nint main() { std::cout << 'Hello, World!'; return 0; }",
    "c": "// Sample C Code\n#include <stdio.h>\nint main() { printf('Hello, World!'); return 0; }",
    "bash": "#!/bin/bash\necho 'Hello, World!'",
    "powershell": "Write-Host 'Hello, World!'",
    "yaml": "key: value\nlist:\n  - item1\n  - item2",
    "toml": "[section]\nkey = 'value'",
    "sql": "SELECT * FROM sample_table;",
    "data": "binarydata",
    "binary": "binarycontent",
    "image": None,  # Placeholder
    "video": None,  # Placeholder
    "audio": None,  # Placeholder
    "pdf": None,  # Placeholder
}

# Function to create files
def create_files(directory, num_files, file_types, sample_content):
    created_files = 0
    logging.info(f"Creating {num_files} files in {directory}...")
    while created_files < num_files:
        file_type = random.choice(list(file_types.keys()))
        extension = file_types[file_type]
        file_name = f"file_{created_files + 1}{extension}"
        file_path = os.path.join(directory, file_name)
        
        try:
            if file_type in sample_content and sample_content[file_type]:
                # Write sample content to file
                with open(file_path, "w") as f:
                    f.write(sample_content[file_type])
            else:
                # Create placeholder binary/image/video/audio/pdf files
                with open(file_path, "wb") as f:
                    f.write(os.urandom(1024))  # Write 1KB of random data
            
            created_files += 1
            logging.debug(f"Created file: {file_path}")
        except Exception as e:
            logging.error(f"Failed to create file: {file_path}. Error: {e}")
            break

# Generate 100 files in each directory
logging.info("Starting file generation...")
create_files(dir1, 100, file_types, sample_content)
create_files(dir2, 100, file_types, sample_content)

logging.info(f"File generation complete. Check the directories:\n{dir1}\n{dir2}")
