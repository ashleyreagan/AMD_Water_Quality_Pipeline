# pa_cleanup_unify.py
import pandas as pd, re

# 1️⃣ Load dataset
df = pd.read_parquet("data_outputs/PA_wq_joined_mine_hydro.parquet")

# 2️⃣ Drop obvious garbage columns but KEEP Bituminous_dist_m + index_right
drop_patterns = r'(?i)(in$|out$|_left$|_right$|maintenanc|organization|monitoring|mineral|point_)'
keep_exceptions = ["Bituminous_dist_m", "index_right"]

drop_cols = [
    c for c in df.columns
    if re.search(drop_patterns, c)
    and c not in keep_exceptions
]

df = df.drop(columns=drop_cols, errors="ignore")

# 3️⃣ Recreate unified chemistry fields if they exist as “In/Out” pairs
def unify_field(prefix):
    cols = [c for c in df.columns if re.fullmatch(prefix + r'(?i)(In|Out)', c)]
    if not cols:
        return
    s = pd.concat([pd.to_numeric(df[c], errors="coerce") for c in cols], axis=1)
    df[prefix] = s.mean(axis=1, skipna=True)

for analyte in ["pH", "Iron", "Temp", "Cond", "Sulfate"]:
    unify_field(analyte)

# 4️⃣ Optional sanity cleanup: drop now-empty cols
df = df.dropna(axis=1, how="all")

# 5️⃣ Save new parquet
out_path = "data_outputs/PA_wq_joined_mine_hydro_clean.parquet"
df.to_parquet(out_path)
print(f"✅ Clean dataset saved to {out_path}")
print(f"Columns now retained: {list(df.columns)[:20]} … ({df.shape[1]} total)")