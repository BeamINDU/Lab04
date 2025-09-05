# run_tests.py
#!/usr/bin/env python

import subprocess
import sys

def run_tests():
    """Run all test suites"""
    
    print("🧪 Running Unit Tests...")
    result = subprocess.run(["pytest", "-m", "unit", "-v"], capture_output=True)
    if result.returncode != 0:
        print("❌ Unit tests failed!")
        return False
    
    print("🔗 Running Integration Tests...")
    result = subprocess.run(["pytest", "-m", "integration"], capture_output=True)
    if result.returncode != 0:
        print("❌ Integration tests failed!")
        return False
    
    print("⚡ Running Performance Tests...")
    result = subprocess.run(["pytest", "tests/performance"], capture_output=True)
    if result.returncode != 0:
        print("⚠️ Performance tests failed (non-blocking)")
    
    print("✅ All critical tests passed!")
    return True

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)