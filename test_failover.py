import subprocess


def run(*args):
 subprocess.run(["docker", "compose", *args], check=True)


if __name__ == "__main__":
 run("up", "-d", "--build", "--wait", "--wait-timeout", "600")
 try:
  run("stop", "kafka1")
  run("run", "--rm", "--no-deps", "tests", "pytest", "-q", "tests/test_pipeline.py::test_pipeline_metrics_export")
 finally:
  run("start", "kafka1")
