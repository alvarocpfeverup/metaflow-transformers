from dataclasses import dataclass
from typing import Optional

from buz.event import Event


@dataclass(frozen=True)
class SessionChannelPriceChangeEvent(Event):
    session_id: int
    channel_ids: list[str]
    currency: str
    requester_id: int
    ticket_price: Optional[float] = None
    ticket_price_tax_base: Optional[float] = None
    surcharge_per_ticket: Optional[float] = None
    surcharge_per_ticket_tax_base: Optional[float] = None
    strikethrough_price: Optional[float] = None

    @classmethod
    def fqn(cls) -> str:
        return "event.fever2.core.session_price.session_price_change_request"
