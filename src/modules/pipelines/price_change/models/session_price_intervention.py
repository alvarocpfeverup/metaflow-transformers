from typing import Optional

from pydantic import BaseModel, Field


class SessionPriceIntervention(BaseModel):
    id_plan: int = Field(alias="ID_PLAN")
    id_session: int = Field(alias="ID_SESSION")
    current_price: float = Field(alias="CURRENT_PRICE")
    new_price: float = Field(alias="NEW_PRICE")
    must_use_tax_base: bool = Field(alias="MUST_USE_TAX_BASE")
    currency_smallest_unit: int = Field(alias="NM_SMALL_UNIT_CONV")
    currency: str = Field(alias="CURRENCY")
    channel_ids: str = Field(alias="CHANNEL_IDS")
    intervention_label: str = Field(alias="INTERVENTION_LABEL")
    price_change_info: Optional[str] = Field(alias="PRICE_CHANGE_INFO")

    class Config:
        allow_population_by_field_name = True
