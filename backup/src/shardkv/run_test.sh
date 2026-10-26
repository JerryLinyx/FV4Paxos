#!/bin/bash
# Usage:
#   ./run_tests.sh [TOTAL_runs] [TestName1 [TestName2 ...]]
# Examples:
#   ./run_tests.sh           # run all tests 100 times
#   ./run_tests.sh 50        # run all tests 50 times
#   ./run_tests.sh 30 TestX  # run only tests containing "TestX"

PASSED=0
FAILED=0
TOTAL="${1:-100}"

# ensure go env matches course layout (GOPATH mode)
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
export GO111MODULE=off
export GOPATH="$ROOT"

if ! [[ "$TOTAL" =~ ^[0-9]+$ ]]; then
  echo "Warning: '$TOTAL' not numeric, using default TOTAL=100"
  TOTAL=100
  NAMES=("$@")
else
  shift || true
  NAMES=("$@")
fi

if [ "${#NAMES[@]}" -eq 0 ]; then
  TESTS="."
else
  TESTS="^($(IFS='|'; echo "${NAMES[*]}"))"
fi

mkdir -p logs
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'

for i in $(seq 1 $TOTAL); do
    echo -n "[$(date '+%H:%M:%S')] Run $i/$TOTAL: "
    go clean -testcache >/dev/null 2>&1

    if go test -run "$TESTS" -count=1 -timeout 1m -v > logs/test_$i.log 2>&1; then
        echo -e "${GREEN}PASSED${NC}"
        ((PASSED++))
        rm logs/test_$i.log
    else
        echo -e "${RED}FAILED${NC}"
        ((FAILED++))
    fi
done

echo -e "\n===== Summary ====="
echo "Total: $TOTAL"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo "Success rate: $(( PASSED * 100 / TOTAL ))%"

if [ $FAILED -gt 0 ]; then
    echo -e "\nCheck failed logs in ./logs/"
else
    rm -rf logs
    echo -e "\nAll tests passed ✅"
fi
