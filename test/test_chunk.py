import os
import subprocess
import tempfile
import pytest
from bin import chunk

@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname

def test_chunk_script(temp_dir):
    input_file_content = "This is the content of the input file."
    input_file_path = os.path.join(temp_dir, "test_input_file.txt")
    with open(input_file_path, "w") as f:
        f.write(input_file_content)

    result = subprocess.run(["python3", os.path.join(os.path.dirname(chunk.__file__), "chunk.py"), "-i", input_file_path], capture_output=True, text=True)

    assert result.returncode == 0, "Script execution failed"
    output_dir = os.path.join(temp_dir, "chunk")
    assert os.path.exists(output_dir), "Output directory not created"

    manifest_file_path = os.path.join(output_dir, "manifest.json")
    assert os.path.exists(manifest_file_path), "Manifest file not created"

    assert os.path.exists(os.path.join(output_dir, "00000_d7fbd01c6e0e4cc5a5a9d1e6570f364b.part")), "Output chunk file does not exist"
