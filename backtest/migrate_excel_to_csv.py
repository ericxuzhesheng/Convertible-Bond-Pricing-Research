"""
一次性历史数据同步与持久化脚本 (migrate_excel_to_csv.py)

目的：
    将 Legacy Excel 文件（【浙商固收】转债资产端特征数据库【周更新外发】.xlsx 和 转债错误定价数据.xlsx）
    中的历史时间序列及转债特征数据，对齐并合并入 data_pipeline.py 维护的 CSV 缓存文件中。
    通过 `combine_first` 保证既有 CSV 缓存不被覆盖，仅将 CSV 中缺失的历史数据/早年字段增补入库。

运行规则：
    python backtest/migrate_excel_to_csv.py
"""

import os
import sys
import io
import pandas as pd
import numpy as np

# 确保控制台支持 UTF-8 打印
if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_DB_PATH = os.path.join(SCRIPT_DIR, "【浙商固收】转债资产端特征数据库【周更新外发】.xlsx")
EXCEL_REDEMPTION_PATH = os.path.join(SCRIPT_DIR, "转债错误定价数据.xlsx")

def _normalize_code(code: str) -> str:
    """标准化转债代码为 XXXXXX.SH / XXXXXX.SZ"""
    if not isinstance(code, str) or pd.isna(code):
        return ""
    code = code.strip()
    if "." in code:
        parts = code.split(".")
        if len(parts[0]) == 6 and parts[1] in ("SH", "SZ"):
            return f"{parts[0]}.{parts[1]}"
    # 纯数字推断
    if len(code) == 6 and code.isdigit():
        if code.startswith(("11", "10", "13", "12", "18")):
            if code.startswith(("11", "13", "10")):
                return f"{code}.SH"
            else:
                return f"{code}.SZ"
    return code

def migrate_wide_sheet(sheet_name: str, csv_filename: str, header_row: int = 0, date_col: int = 0, code_row: int = None, data_start_row: int = 1, col_start: int = 1):
    """迁移宽表Sheet到CSV缓存"""
    csv_path = os.path.join(SCRIPT_DIR, csv_filename)
    print(f"正在同步 Sheet [{sheet_name}] -> [{csv_filename}]...")
    
    if not os.path.exists(EXCEL_DB_PATH):
        print(f"   未找到 Excel 文件 {EXCEL_DB_PATH}，跳过。")
        return

    try:
        if code_row is not None:
            df_raw = pd.read_excel(EXCEL_DB_PATH, sheet_name=sheet_name, header=None, engine="openpyxl")
            dates = pd.to_datetime(df_raw.iloc[data_start_row:, date_col].tolist(), errors="coerce")
            codes = [_normalize_code(str(c)) for c in df_raw.iloc[code_row, col_start:].tolist()]
            data = df_raw.iloc[data_start_row:, col_start:].values
            df_excel = pd.DataFrame(data, index=dates, columns=codes)
        else:
            df_excel = pd.read_excel(EXCEL_DB_PATH, sheet_name=sheet_name, header=header_row, index_col=date_col, engine="openpyxl")
            df_excel.index = pd.to_datetime(df_excel.index, errors="coerce")
            df_excel.columns = [_normalize_code(str(c)) for c in df_excel.columns]

        df_excel = df_excel[df_excel.index.notnull()]
        df_excel = df_excel.loc[:, [c for c in df_excel.columns if c]]
        df_excel = df_excel[~df_excel.index.duplicated(keep="first")].sort_index()

        if os.path.exists(csv_path):
            df_csv = pd.read_csv(csv_path, index_col=0)
            df_csv.index = pd.to_datetime(df_csv.index, errors="coerce")
            df_csv = df_csv[df_csv.index.notnull()]
            old_rows, old_cols = df_csv.shape
            
            merged = df_csv.combine_first(df_excel)
            merged = merged.sort_index()
            new_rows, new_cols = merged.shape
            print(f"   同步完成: {csv_filename} (行数 {old_rows}->{new_rows}, 列数 {old_cols}->{new_cols})")
        else:
            merged = df_excel.sort_index()
            print(f"   新建缓存: {csv_filename} (共 {merged.shape[0]} 行, {merged.shape[1]} 列)")

        merged.to_csv(csv_path)
    except Exception as e:
        print(f"   同步 Sheet [{sheet_name}] 失败: {e}")

def migrate_bps():
    """同步每股净资产 BPS"""
    sheet_name = "每股净资产"
    csv_filename = "cb_bps_cache.csv"
    csv_path = os.path.join(SCRIPT_DIR, csv_filename)
    print(f"正在同步 Sheet [{sheet_name}] -> [{csv_filename}]...")
    
    if not os.path.exists(EXCEL_REDEMPTION_PATH):
        print(f"   未找到 {EXCEL_REDEMPTION_PATH}，跳过。")
        return

    try:
        df_raw = pd.read_excel(EXCEL_REDEMPTION_PATH, sheet_name=sheet_name, header=4, index_col=0, engine="openpyxl")
        df_raw = df_raw.iloc[1:]
        df_raw.index = pd.to_datetime(df_raw.index, errors="coerce")
        df_raw = df_raw[~df_raw.index.duplicated(keep="first")].sort_index()
        df_raw = df_raw[df_raw.index.notnull()]
        
        if os.path.exists(csv_path):
            df_csv = pd.read_csv(csv_path, index_col=0)
            df_csv.index = pd.to_datetime(df_csv.index, errors="coerce")
            df_csv = df_csv[df_csv.index.notnull()]
            old_rows, old_cols = df_csv.shape
            
            df_excel = df_raw.copy()
            df_excel.columns = [_normalize_code(str(c)) for c in df_excel.columns]
            
            merged = df_csv.combine_first(df_excel)
            merged = merged.sort_index()
            new_rows, new_cols = merged.shape
            print(f"   同步完成: {csv_filename} (行数 {old_rows}->{new_rows}, 列数 {old_cols}->{new_cols})")
            merged.to_csv(csv_path)
    except Exception as e:
        print(f"   同步 BPS 失败: {e}")

def migrate_redemption_price():
    """同步到期赎回价至 cb_basic_info.csv"""
    sheet_name = "到期赎回价"
    csv_filename = "cb_basic_info.csv"
    csv_path = os.path.join(SCRIPT_DIR, csv_filename)
    print(f"正在同步到期赎回价 [{sheet_name}] -> [{csv_filename}]...")

    if not os.path.exists(EXCEL_REDEMPTION_PATH) or not os.path.exists(csv_path):
        print("   文件不存在，跳过。")
        return

    try:
        df_red = pd.read_excel(EXCEL_REDEMPTION_PATH, sheet_name=sheet_name, engine="openpyxl")
        col_code = next((c for c in df_red.columns if "代码" in str(c)), None)
        col_price = next((c for c in df_red.columns if "赎回价" in str(c)), None)
        if not col_code or not col_price:
            print("   未找到代码或赎回价列。")
            return
        
        df_red[col_code] = df_red[col_code].apply(lambda x: _normalize_code(str(x)))
        red_map = df_red.set_index(col_code)[col_price].to_dict()
        
        df_basic = pd.read_csv(csv_path)
        if "maturity_price" not in df_basic.columns:
            df_basic["maturity_price"] = np.nan
            
        filled_count = 0
        for idx, row in df_basic.iterrows():
            code = row["ts_code"]
            if pd.isna(row["maturity_price"]) and code in red_map and pd.notna(red_map[code]):
                df_basic.at[idx, "maturity_price"] = red_map[code]
                filled_count += 1
                
        print(f"   增补完成了 {filled_count} 条转债到期赎回价（现有总计有效 {df_basic['maturity_price'].notna().sum()} 条）")
        df_basic.to_csv(csv_path, index=False)
    except Exception as e:
        print(f"   同步赎回价失败: {e}")

def main():
    print("=== 开始运行 Legacy Excel 历史数据同步向导 ===")
    
    # 1. 宽表同步
    migrate_wide_sheet("可转债价格", "cb_price_cache.csv")
    migrate_wide_sheet("转换价值", "cb_convert_val_cache.csv")
    migrate_wide_sheet("纯债价值", "cb_bond_floor_cache.csv")
    migrate_wide_sheet("剩余期限", "cb_maturity_cache.csv")
    migrate_wide_sheet("正股市值", "cb_stock_mv_cache.csv")
    
    # 2. 复杂表头宽表同步（mispricing factor 使用）
    migrate_wide_sheet("可转债交易额", "cb_amount_cache.csv", code_row=3, data_start_row=5, date_col=0, col_start=1)
    migrate_wide_sheet("信用评级", "cb_rating_cache.csv", code_row=2, data_start_row=4, date_col=2, col_start=3)
    migrate_wide_sheet("可转债余额", "cb_balance_cache.csv", code_row=3, data_start_row=5, date_col=0, col_start=1)
    
    # 3. BPS 与到期赎回价同步
    migrate_bps()
    migrate_redemption_price()
    
    print("=== 历史数据同步向导运行完毕 ===")

if __name__ == "__main__":
    main()
