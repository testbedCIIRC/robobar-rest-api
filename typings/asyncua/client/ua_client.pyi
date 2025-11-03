import asyncio
from collections.abc import Awaitable, Callable

from asyncua import ua
from asyncua.common.session_interface import AbstractSession
from common.connection import TransportLimits
from crypto import security_policies
from ua.uaprotocol_auto import OpenSecureChannelResult

"""
Low level binary client
"""

class UASocketProtocol(asyncio.Protocol):
    """Handle socket connection and send ua messages.
    Timeout is the timeout used while waiting for an ua answer from server.
    """

    INITIALIZED = ...
    OPEN = ...
    CLOSED = ...
    def __init__(
        self,
        timeout: float = ...,
        security_policy: security_policies.SecurityPolicy = ...,
        limits: TransportLimits = ...,
    ) -> None:
        """:param timeout: Timeout in seconds
        :param security_policy: Security policy (optional)
        """

    def connection_made(self, transport: asyncio.Transport):  # -> None:
        ...
    def connection_lost(self, exc: Exception | None):  # -> None:
        ...
    def data_received(self, data: bytes) -> None: ...
    async def send_request(self, request, timeout: float | None = ..., message_type=...):
        """Send a request to the server.
        Timeout is the timeout written in ua header.
        Returns response object if no callback is provided.
        """

    def check_answer(self, data, context):  # -> bool:
        ...
    def disconnect_socket(self):  # -> None:
        ...
    async def send_hello(self, url, max_messagesize: int = ..., max_chunkcount: int = ...): ...
    async def open_secure_channel(self, params) -> OpenSecureChannelResult: ...
    async def close_secure_channel(self):  # -> None:
        """Close secure channel.
        It seems to trigger a shutdown of socket in most servers, so be prepared to reconnect.
        OPC UA specs Part 6, 7.1.4 say that Server does not send a CloseSecureChannel response
        and should just close socket.
        """

class UaClient(AbstractSession):
    """low level OPC-UA client.

    It implements (almost) all methods defined in asyncua spec
    taking in argument the structures defined in asyncua spec.

    In this Python implementation  most of the structures are defined in
    uaprotocol_auto.py and uaprotocol_hand.py available under asyncua.ua
    """

    def __init__(self, timeout: float = ...) -> None:
        """:param timeout: Timout in seconds"""

    def set_security(self, policy: security_policies.SecurityPolicy):  # -> None:
        ...
    @property
    def pre_request_hook(self) -> Callable[[], Awaitable[None]] | None: ...
    @pre_request_hook.setter
    def pre_request_hook(self, hook: Callable[[], Awaitable[None]] | None):  # -> None:
        ...
    async def connect_socket(self, host: str, port: int):  # -> None:
        """Connect to server socket."""

    def disconnect_socket(self):  # -> None:
        ...
    async def send_hello(self, url, max_messagesize: int = ..., max_chunkcount: int = ...):  # -> None:
        ...
    async def open_secure_channel(self, params):  # -> OpenSecureChannelResult:
        ...
    async def close_secure_channel(self):  # -> None:
        """Close secure channel. It seems to trigger a shutdown of socket
        in most servers, so be prepared to reconnect
        """

    async def create_session(self, parameters):  # -> CreateSessionResult:
        ...
    async def activate_session(self, parameters):  # -> ActivateSessionResult:
        ...
    async def close_session(self, delete_subscriptions):  # -> None:
        ...
    async def browse(self, parameters):  # -> List[BrowseResult]:
        ...
    async def browse_next(self, parameters):  # -> List[BrowseResult]:
        ...
    async def read(self, parameters):  # -> List[DataValue]:
        ...
    async def write(self, params):  # -> List[StatusCode]:
        ...
    async def get_endpoints(self, params):  # -> List[EndpointDescription]:
        ...
    async def find_servers(self, params):  # -> List[ApplicationDescription]:
        ...
    async def find_servers_on_network(self, params):  # -> FindServersOnNetworkResult:
        ...
    async def register_server(self, registered_server):  # -> None:
        ...
    async def unregister_server(self, registered_server):  # -> None:
        ...
    async def register_server2(self, params):  # -> List[StatusCode]:
        ...
    async def unregister_server2(self, params):  # -> List[StatusCode]:
        ...
    async def translate_browsepaths_to_nodeids(self, browse_paths):  # -> List[BrowsePathResult]:
        ...
    async def create_subscription(
        self,
        params: ua.CreateSubscriptionParameters,
        callback,
    ) -> ua.CreateSubscriptionResult: ...
    async def inform_subscriptions(self, status: ua.StatusCode):  # -> None:
        """Inform all current subscriptions with a status code. This calls the handler's status_change_notification"""

    async def update_subscription(self, params: ua.ModifySubscriptionParameters) -> ua.ModifySubscriptionResult: ...

    modify_subscription = ...
    async def delete_subscriptions(self, subscription_ids):  # -> List[StatusCode]:
        ...
    async def publish(self, acks: list[ua.SubscriptionAcknowledgement]) -> ua.PublishResponse:
        """Send a PublishRequest to the server."""

    async def create_monitored_items(self, params):  # -> List[MonitoredItemCreateResult]:
        ...
    async def delete_monitored_items(self, params):  # -> List[StatusCode]:
        ...
    async def add_nodes(self, nodestoadd):  # -> List[AddNodesResult]:
        ...
    async def add_references(self, refs):  # -> List[StatusCode]:
        ...
    async def delete_references(self, refs):  # -> List[StatusCode]:
        ...
    async def delete_nodes(self, params):  # -> List[StatusCode]:
        ...
    async def call(self, methodstocall):  # -> List[CallMethodResult]:
        ...
    async def history_read(self, params):  # -> List[HistoryReadResult]:
        ...
    async def modify_monitored_items(self, params):  # -> List[MonitoredItemModifyResult]:
        ...
    async def register_nodes(self, nodes):  # -> List[NodeId]:
        ...
    async def unregister_nodes(self, nodes):  # -> None:
        ...
    async def read_attributes(self, nodeids, attr):  # -> List[DataValue]:
        ...
    async def write_attributes(self, nodeids, datavalues, attributeid=...):  # -> List[StatusCode]:
        """Set an attribute of multiple nodes
        datavalue is a ua.DataValue object
        """

    async def set_monitoring_mode(self, params) -> list[ua.uatypes.StatusCode]:
        """Update the subscription monitoring mode"""

    async def set_publishing_mode(self, params) -> list[ua.uatypes.StatusCode]:
        """Update the subscription publishing mode"""

    async def transfer_subscriptions(self, params: ua.TransferSubscriptionsParameters) -> list[ua.TransferResult]: ...
