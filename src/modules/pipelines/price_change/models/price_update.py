from dataclasses import dataclass
from typing import ClassVar, Optional


@dataclass(frozen=True)
class PriceUpdate:
    id_plan: int
    id_session: int
    intervention_label: str
    current_price: float
    new_price: float
    must_use_tax_base: bool
    currency: str
    channel_ids: list[str]
    currency_smallest_unit: int
    price_change_info: Optional[str]

    RATIO_MINIMUM: ClassVar[float] = 0.5
    RATIO_MAXIMUM: ClassVar[float] = 2

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if self.is_new_price_zero_or_negative():
            raise ValueError(f"New price can't be zero or negative: {self}")
        if self.is_price_change_ratio_too_extreme():
            raise ValueError(f"The ratio between new and current price is too extreme: {self}")
        if self.is_new_price_granularity_incorrect():
            raise ValueError(f"New price granurality is not correct: {self}")

    def is_new_price_zero_or_negative(self) -> bool:
        return self.new_price <= 0

    def is_price_change_ratio_too_extreme(self) -> bool:
        ratio = self.new_price / self.current_price
        return ratio < self.RATIO_MINIMUM or ratio > self.RATIO_MAXIMUM

    def is_new_price_granularity_incorrect(self) -> bool:
        # a smallest unit of 100 means there are cents
        # a smallest unit of 1 means only whole currency is allowed (e.g. japanese yens)
        return not float(self.new_price * self.currency_smallest_unit).is_integer()
