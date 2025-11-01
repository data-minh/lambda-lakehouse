import os
import time
import pandas as pd
from tqdm import tqdm
from vnstock import Trading, Quote, Listing, Company
import concurrent.futures

# Dùng fsspec mỗi lần mở S3 thay vì giữ client global (tránh lỗi fork-safe)
import fsspec

# --- CỜ LƯU RA S3 ---
USE_S3 = True

# --- THÔNG SỐ S3/MINIO (đọc từ ENV Airflow/worker) ---
S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", "http://minio:9000/")
S3_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

S3_BUCKET = "lakehouse"
S3_PREFIX = "raw/vnstock/vnstock_finally"  # không có slash đầu/cuối

# Nếu vẫn muốn lưu local khi USE_S3=False
OUTPUT_DIR = "./stock-data/vnstock/vnstock_finally/"

# storage_options dùng lại cho mọi thao tác với fsspec/pandas
STORAGE_OPTS = {
    "key": S3_KEY,
    "secret": S3_SECRET,
    "client_kwargs": {"endpoint_url": S3_ENDPOINT},
}

# ===================== TIỆN ÍCH S3 (an toàn với fork) =====================

def s3_path_for_trading_date(trading_date: str) -> str:
    return f"s3://{S3_BUCKET}/{S3_PREFIX}/{trading_date}.csv"

def s3_exists(path: str) -> bool:
    fs, _, paths = fsspec.get_fs_token_paths(path, storage_options=STORAGE_OPTS)
    return fs.exists(paths[0])

def s3_read_csv(path: str) -> pd.DataFrame:
    # pandas sẽ tự tạo FS mới qua storage_options mỗi lần -> fork-safe
    return pd.read_csv(path, storage_options=STORAGE_OPTS)

def s3_write_csv(df: pd.DataFrame, path: str, header: bool):
    # Ghi append bằng cách mở lại object mỗi lần
    mode = "wb" if header else "ab"
    with fsspec.open(path, mode, **STORAGE_OPTS) as f:
        df.to_csv(f, header=header, index=False, encoding="utf-8-sig")

# ===================== TIỆN ÍCH LOCAL =====================

def local_exists(path: str) -> bool:
    return os.path.exists(path)

def local_read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def local_write_csv(df: pd.DataFrame, path: str, header: bool):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(
        path,
        mode=("w" if header else "a"),
        header=header,
        index=False,
        encoding="utf-8-sig",
    )

# ===================== LOGIC ETL =====================

def create_chunks(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def fetch_data(symbol):
    overview_df = None
    profile_df = None
    try:
        company = Company(symbol=symbol, source='TCBS')
        try:
            overview_df = company.overview()
            if overview_df.empty:
                overview_df = None
        except Exception:
            pass
        try:
            profile_df = company.profile()
            if not profile_df.empty:
                profile_df['symbol'] = symbol
            else:
                profile_df = None
        except Exception:
            pass
    except Exception:
        pass
    return (symbol, overview_df, profile_df)

def save_batch_to_csv(results_in_chunk, price_df_full, output_path, is_first_save, use_s3: bool):
    # 1) TÁCH KẾT QUẢ
    all_symbols_in_batch = [symbol for symbol, _, _ in results_in_chunk]
    all_overviews = [ov_df for _, ov_df, _ in results_in_chunk if ov_df is not None]
    all_profiles = [pr_df for _, _, pr_df in results_in_chunk if pr_df is not None]

    if not all_symbols_in_batch:
        print("\nLô không có kết quả, bỏ qua lưu.")
        return

    # 2) CHUẨN BỊ MERGE
    price_batch_df = price_df_full[price_df_full['symbol'].isin(all_symbols_in_batch)].copy()
    overview_batch_df = pd.concat(all_overviews, ignore_index=True) if all_overviews else pd.DataFrame()
    profile_batch_df = pd.concat(all_profiles, ignore_index=True) if all_profiles else pd.DataFrame()

    # 3) MERGE
    merged_df = price_batch_df
    if not overview_batch_df.empty:
        merged_df = pd.merge(
            merged_df,
            overview_batch_df,
            on='symbol',
            how='left',
            suffixes=('_price', '_overview')
        )
    if not profile_batch_df.empty:
        merged_df = pd.merge(
            merged_df,
            profile_batch_df,
            on='symbol',
            how='left',
            suffixes=('_overview', '_profile')
        )

    # 4) ĐIỀN "Not found"
    price_cols = set(price_batch_df.columns)
    merged_cols = set(merged_df.columns)
    new_cols = list(merged_cols - price_cols)
    if new_cols:
        merged_df[new_cols] = merged_df[new_cols].fillna("Not found")
    else:
        print("\nKhông có cột mới nào được thêm (tất cả mã trong lô này đều lỗi).")

    # 5) LƯU
    if use_s3:
        s3_write_csv(merged_df, output_path, header=is_first_save)
    else:
        local_write_csv(merged_df, output_path, header=is_first_save)

    print(f"\nĐã lưu {len(merged_df)} mã trong lô vào {'S3' if use_s3 else 'file'} {output_path} (kể cả mã lỗi)")

def vnstock_ingestion(
    CHUNK_SIZE_DETAIL: int,
    MAX_WORKERS: int,
    PAUSE_TIME_SUCCESS: int,
    PAUSE_TIME_CRASH: int
) -> None:

    print("--- Bắt đầu script ETL ---")
    if not USE_S3:
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1) Lấy danh sách mã
    try:
        listing_obj = Listing()
        df_symbols = listing_obj.all_symbols()
        vnstock_list = df_symbols['symbol'].to_list()
        print(f"Tìm thấy {len(vnstock_list)} mã. Bắt đầu tải...")
    except Exception as e:
        print(f"Không thể lấy danh sách mã: {e}")
        vnstock_list = []

    # 2) Lấy bảng giá
    trading = Trading()
    data_df = trading.price_board(symbols_list=vnstock_list)['listing']

    # 2b) Xác định output path
    trading_date = data_df['trading_date'].iloc[0]
    if USE_S3:
        output_path = s3_path_for_trading_date(trading_date)
    else:
        output_path = os.path.join(OUTPUT_DIR, f'{trading_date}.csv')

    print(f"File output cho hôm nay là: {output_path}")

    # 3) Vòng lặp retry
    while True:
        full_ticker_list = data_df['symbol'].unique()
        processed_symbols = set()
        is_first_save = True

        # Đọc file đã xử lý (nếu có)
        try:
            if USE_S3:
                if s3_exists(output_path):
                    print(f"Đang đọc file đã xử lý (S3): {output_path}")
                    processed_df = s3_read_csv(output_path)
                    processed_symbols = set(processed_df['symbol'].unique())
                    is_first_save = False
                    print(f"Đã tìm thấy {len(processed_symbols)} mã đã xử lý.")
            else:
                if local_exists(output_path):
                    print(f"Đang đọc file đã xử lý (local): {output_path}")
                    processed_df = local_read_csv(output_path)
                    processed_symbols = set(processed_df['symbol'].unique())
                    is_first_save = False
                    print(f"Đã tìm thấy {len(processed_symbols)} mã đã xử lý.")
        except pd.errors.EmptyDataError:
            print("File output rỗng, sẽ ghi đè.")
            is_first_save = True
        except Exception as e:
            print(f"Lỗi đọc file CSV: {e}. Sẽ thử lại sau 10s...")
            time.sleep(10)
            continue

        remaining_tickers = [t for t in full_ticker_list if t not in processed_symbols]
        if not remaining_tickers:
            print(f"--- DỮ LIỆU NGÀY {trading_date} ĐÃ HOÀN TẤT ---")
            break

        print(f"Tổng: {len(full_ticker_list)} | Đã xử lý: {len(processed_symbols)} | Còn lại: {len(remaining_tickers)}")

        ticker_chunks = list(create_chunks(remaining_tickers, CHUNK_SIZE_DETAIL))

        try:
            for chunk in tqdm(ticker_chunks, desc=f"Đang xử lý {len(remaining_tickers)} mã còn lại"):
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    results_in_chunk = list(executor.map(fetch_data, chunk))

                save_batch_to_csv(results_in_chunk, data_df, output_path, is_first_save, USE_S3)
                is_first_save = False

                print(f"--- Đã lưu lô. Tạm nghỉ {PAUSE_TIME_SUCCESS} giây (thành công)...")
                time.sleep(PAUSE_TIME_SUCCESS)

            print("--- Vòng lặp for hoàn tất (không crash) ---")

        except SystemExit as e:
            print(f"\n--- !!! LỖI RATE LIMIT !!! ---")
            print(f"Lỗi: {e}")
            print(f"Script bị TCBS chặn. Sẽ tự động thử lại sau {PAUSE_TIME_CRASH} giây...")
            time.sleep(PAUSE_TIME_CRASH)

        except Exception as e:
            print(f"\n--- !!! LỖI BẤT NGỜ !!! ---")
            print(f"Lỗi: {e}. Sẽ thử lại sau {PAUSE_TIME_CRASH} giây...")
            time.sleep(PAUSE_TIME_CRASH)
