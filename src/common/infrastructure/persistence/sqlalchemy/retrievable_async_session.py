from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction


class RetrievableAsyncSession(AsyncSession):
    # This class helps to retrieve the transaction when the session.begin() method is not used as context
    # manager. When begin is used but not as context manager we won't be able to retrieve the transaction because the
    # initialization of the transaction is different. AsyncSession also have a sync_session and the transaction is
    # retrieved from that sync session by using some reverse proxy classes that are initialized when the transaction
    # begins (but this only happens when it is used as context manager).
    # This might to be a bug for AsyncSession (SqlAlchemy).

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__first_transaction = None

    def begin(self) -> AsyncSessionTransaction:
        self.__first_transaction = super().begin()
        return self.__first_transaction

    def get_transaction(self) -> Optional[AsyncSessionTransaction]:
        try:
            transaction = super().get_transaction()
        except NotImplementedError:
            transaction = self.__first_transaction

        return transaction

    async def close(self) -> None:
        try:
            await super().close()
        except Exception as exception:
            raise exception
        finally:
            self.__first_transaction = None
