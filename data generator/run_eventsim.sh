#!/bin/bash

# 1. Định nghĩa các giá trị mặc định (Default values)
# Nếu bạn không truyền gì vào, nó sẽ lấy các giá trị này
DEFAULT_DATA_DIR="./streamify_raw_data"
DEFAULT_USERS=1000
IMAGE="viirya/eventsim:latest"

# 2. Gán giá trị từ tham số dòng lệnh ($1 là tham số đầu tiên, $2 là thứ hai)
# Cú pháp ${1:-$DEFAULT} nghĩa là: Lấy $1, nếu $1 trống thì lấy DEFAULT
DATA_DIR="${1:-$DEFAULT_DATA_DIR}"
USERS="${2:-$DEFAULT_USERS}"

ABS_DATA_DIR=$(realpath -m "$DATA_DIR")

echo "------------------------------------------"
echo "📂 Export data location: $ABS_DATA_DIR"
echo "👤 Number of user: $USERS"
echo "------------------------------------------"

# Đảm bảo thư mục tồn tại
mkdir -p "$ABS_DATA_DIR"

# Chạy Podman
podman run --rm \
  -v "$ABS_DATA_DIR:/data:Z" \
  "$IMAGE" \
  -c "examples/example-config.json" \
  --dataset song \
  --dir /data \
  --nusers "$USERS" \
  --format json

echo "✅ Finished! Check data at: $ABS_DATA_DIR"