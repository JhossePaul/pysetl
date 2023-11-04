from pysetl.enums import SaveMode
from pysetl.config import CsvConfig, ParquetConfig


SETTINGS = {
    "citizens": CsvConfig(
        path="/content/citizens.csv",
        inferSchema="true",
        savemode=SaveMode.OVERWRITE
    ),
    "cities": ParquetConfig(
        path="/content/city.parquet",
        savemode=SaveMode.ERRORIFEXISTS
    )
}
