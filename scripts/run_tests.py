import subprocess
import sys

result = subprocess.run(
    [sys.executable, "-m", "pytest", "--cov=scripts", "--cov-fail-under=70"],
    text=True
)

sys.exit(result.returncode)
