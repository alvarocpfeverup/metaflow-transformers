from pydantic import BaseModel, Field


class RevertCampaignData(BaseModel):
    id_campaign_plan: int = Field(alias="ID_CAMPAIGN_PLAN")
    main_plan_id: int = Field(alias="MAIN_PLAN_ID")
    session_id: int | None = Field(alias="SESSION_ID")
    partner_id: int = Field(alias="PARTNER_ID")
    original_ticket_price: float | None = Field(alias="ORIGINAL_TICKET_PRICE")
    channel_ids_to_apply: str | None = Field(alias="CHANNEL_IDS_TO_APPLY")
    must_use_tax_base: bool = Field(alias="MUST_USE_TAX_BASE")
    currency: str | None = Field(alias="CURRENCY")
    is_coupon_discount: bool = Field(alias="IS_COUPON_DISCOUNT")
    only_revert_custom_labels: bool = Field(alias="ONLY_REVERT_CUSTOM_LABELS")
    shown_ticket_price: int = -1
