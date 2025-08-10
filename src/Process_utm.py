import pandas as pd
from rapidfuzz import fuzz, process
from src.Get_data_DB import DataTransformer

class ColumnStandardizer:
    def __init__(self, threshold=80, fix_case_map=None, preserve_if_low_similarity=None):
        """
        threshold: ngưỡng tương đồng (0–100)
        fix_case_map: dict viết hoa đặc biệt cần chuẩn lại, ví dụ {'hienpt': 'HienPT'}
        preserve_if_low_similarity: list các giá trị không match nhưng vẫn giữ nguyên
        """
        self.threshold = threshold
        self.fix_case_map = fix_case_map or {
            'hienpt': 'HienPT',
            'thuynth': 'ThuyNTH'
        }
        self.preserve_if_low_similarity = preserve_if_low_similarity or ['quannv']
        self.preserve_if_low_similarity = [x.lower() for x in self.preserve_if_low_similarity]

        self.transformer = DataTransformer()
        self.standard_list, self.standard_map = self._load_standard_usernames()

    def _load_standard_usernames(self):
        """Truy vấn danh sách user_name từ bảng users."""
        query = """
            SELECT user_name
            FROM users
            WHERE (role = 2 AND locked = 0)
                OR name IN ('Bình Ngô', 'Hiền Phạm');
        """
        df_users = self.transformer.fetch_from_mysql(query)
        df_users['user_name_clean'] = df_users['user_name'].str.lower()
        mapping = dict(zip(df_users['user_name_clean'], df_users['user_name']))
        return list(mapping.keys()), mapping

    def _find_best_match(self, value: str):
        """Tìm giá trị giống nhất từ danh sách chuẩn. Trả về 'Khác' nếu không đạt ngưỡng."""
        if pd.isna(value):
            return "Khác"

        original_value = value
        val_lower = str(value).strip().lower()

        result = process.extractOne(val_lower, self.standard_list, scorer=fuzz.ratio)
        if result is None:
            return original_value if val_lower in self.preserve_if_low_similarity else "Khác"

        match, score, _ = result
        if score >= self.threshold:
            return self.standard_map[match]
        else:
            return original_value if val_lower in self.preserve_if_low_similarity else "Khác"

    def transform(self, series: pd.Series) -> pd.Series:
        """Nhận Series → chuẩn hoá & trả về Series mới."""
        matched = series.apply(self._find_best_match)

        # Áp dụng fix viết hoa đặc biệt nếu có
        matched = matched.apply(lambda x: self.fix_case_map.get(x.lower(), x))

        return matched
