name: Fix flow corruption (one-off)

# CHỈ chạy khi bạn tự bấm nút "Run workflow" trên tab Actions — không tự
# động chạy theo lịch, không ảnh hưởng gì tới job fetch_etf.py hàng ngày.
on:
  workflow_dispatch:

jobs:
  fix:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install boto3
        run: pip install boto3

      - name: Run fix script
        env:
          R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}
          R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
          R2_ENDPOINT_URL: ${{ secrets.R2_ENDPOINT_URL }}
          R2_BUCKET_NAME: ${{ secrets.R2_BUCKET_NAME }}
        run: python scripts/audit_r2_data.py
