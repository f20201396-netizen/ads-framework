from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    meta_access_token: str = ""
    meta_app_secret: str = ""
    meta_business_id: str = ""
    meta_ad_account_ids: str = ""  # comma-separated, e.g. "act_111,act_222"
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/meta_ads"
    admin_api_key: str = ""
    frontend_origin: str = "http://localhost:3000"

    # BigQuery / attribution (Phase 6)
    google_application_credentials: str = ""   # path to service-account JSON key
    bq_cost_cap_bytes: int = 5_000_000_000     # 5 GB per query

    # Google Ads
    google_ads_developer_token: str = ""
    google_ads_client_id: str = ""
    google_ads_client_secret: str = ""
    google_ads_refresh_token: str = ""
    google_ads_customer_id: str = ""
    google_ads_login_customer_id: str = ""

    @property
    def ad_account_id_list(self) -> list[str]:
        return [a.strip() for a in self.meta_ad_account_ids.split(",") if a.strip()]

    @property
    def google_ads_customer_id_clean(self) -> str:
        """Returns customer ID without hyphens."""
        return self.google_ads_customer_id.replace("-", "")


settings = Settings()
