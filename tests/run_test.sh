#!/usr/bin/env bash
# cleanup function to ensure background cloudSQL process is terminated
cleanup() {
    if [ -n "$SQL_PID" ]; then
        kill $SQL_PID 2>/dev/null || true
        wait $SQL_PID 2>/dev/null || true
    fi
}

# Trap exit, interrupt and error signals
trap cleanup EXIT INT ERR

# Resolve absolute paths
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
TEST_DATA_DIR="$ROOT_DIR/test_data"

rm -rf "$TEST_DATA_DIR" || true
mkdir -p "$TEST_DATA_DIR"

cd "$BUILD_DIR" || exit 1
make -j$(sysctl -n hw.ncpu)
./cloudSQL -p 5438 -d "$TEST_DATA_DIR" &
SQL_PID=$!
sleep 2

echo "--- Running E2E Tests ---"
python3 "$ROOT_DIR/tests/e2e/e2e_test.py" 5438
RET=$?

if [ $RET -eq 0 ]; then
    echo "--- Running SLT Logic Tests ---"
    for slt_file in "$ROOT_DIR"/tests/logic/*.slt; do
        echo "Running $slt_file..."
        python3 "$ROOT_DIR/tests/logic/slt_runner.py" 5438 "$slt_file"
        SLT_RET=$?
        if [ $SLT_RET -ne 0 ]; then
            RET=$SLT_RET
            break
        fi
    done
fi

exit $RET
