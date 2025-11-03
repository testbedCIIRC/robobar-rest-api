from __future__ import annotations

import asyncio
from datetime import datetime
import functools
import sys
from cryptography import x509
from pathlib import Path
from threading import Thread, Condition
import logging
from typing import Any, Callable, Dict, Iterable, List, Sequence, Set, Tuple, Type, Union, Optional, overload

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from asyncua import ua
from asyncua import client
from asyncua import server
from asyncua import common
from asyncua.common import node, subscription, shortcuts, xmlexporter, type_dictionary_builder
from asyncua.common.events import Event
_logger = ...
class ThreadLoopNotRunning(Exception):
    ...


class ThreadLoop(Thread):
    def __init__(self, timeout: Optional[float] = ...) -> None:
        ...
    
    def start(self): # -> None:
        ...
    
    def run(self): # -> None:
        ...
    
    def stop(self): # -> None:
        ...
    
    def post(self, coro):
        ...
    
    def __enter__(self): # -> Self:
        ...
    
    def __exit__(self, exc_t, exc_v, trace): # -> None:
        ...
    


def syncmethod(func): # -> Callable[..., SyncNode | list[SyncNode | list[Any] | EventGenerator | Subscription | Server | Any] | EventGenerator | Subscription | Server | Any]:
    """
    decorator for sync methods
    """
    ...

def sync_wrapper(aio_func): # -> Callable[..., SyncNode | list[SyncNode | list[Any] | EventGenerator | Subscription | Server | Any] | EventGenerator | Subscription | Server | Any]:
    ...

def syncfunc(aio_func): # -> Callable[..., Callable[..., SyncNode | list[SyncNode | list[Any] | EventGenerator | Subscription | Server | Any] | EventGenerator | Subscription | Server | Any]]:
    """
    decorator for sync function
    """
    ...

def sync_uaclient_method(aio_func): # -> Callable[..., partial[SyncNode | list[SyncNode | list[Any] | EventGenerator | Subscription | Server | Any] | EventGenerator | Subscription | Server | Any]]:
    """
    Usage:

    ```python
    from asyncua.client.ua_client import UaClient
    from asyncua.sync import Client

    with Client('otp.tcp://localhost') as client:
        read_attributes = sync_uaclient_method(UaClient.read_attributes)(client)
        results = read_attributes(...)
        ...
    ```
    """
    ...

def sync_async_client_method(aio_func): # -> Callable[..., partial[SyncNode | list[SyncNode | list[Any] | EventGenerator | Subscription | Server | Any] | EventGenerator | Subscription | Server | Any]]:
    """
    Usage:

    ```python
    from asyncua.client import Client as AsyncClient
    from asyncua.sync import Client

    with Client('otp.tcp://localhost') as client:
        read_attributes = sync_async_client_method(AsyncClient.read_attributes)(client)
        results = read_attributes(...)
        ...
    ```
    """
    ...

@syncfunc(aio_func=common.methods.call_method_full)
def call_method_full(parent, methodid, *args): # -> None:
    ...

@syncfunc(aio_func=common.ua_utils.data_type_to_variant_type)
def data_type_to_variant_type(dtype_node): # -> None:
    ...

@syncfunc(aio_func=common.copy_node_util.copy_node)
def copy_node(parent, node, nodeid=..., recursive=...): # -> None:
    ...

@syncfunc(aio_func=common.instantiate_util.instantiate)
def instantiate(parent, node_type, nodeid=..., bname=..., dname=..., idx=..., instantiate_optional=...): # -> None:
    ...

class _SubHandler:
    def __init__(self, tloop, sync_handler) -> None:
        ...
    
    def datachange_notification(self, node, val, data): # -> None:
        ...
    
    def event_notification(self, event): # -> None:
        ...
    
    def status_change_notification(self, status: ua.StatusChangeNotification): # -> None:
        ...
    


class Client:
    """
    Sync Client, see doc for async Client
    the sync client has one extra parameter: sync_wrapper_timeout.
    if no ThreadLoop is provided this timeout is used to define how long the sync wrapper
    waits for an async call to return. defualt is 120s and hopefully should fit most applications
    """
    def __init__(self, url: str, timeout: float = ..., tloop=..., sync_wrapper_timeout: Optional[float] = ..., watchdog_intervall: float = ...) -> None:
        ...
    
    def __str__(self) -> str:
        ...
    
    __repr__ = ...
    @property
    def application_uri(self): # -> str:
        ...
    
    @application_uri.setter
    def application_uri(self, value): # -> None:
        ...
    
    @syncmethod
    def connect(self) -> None:
        ...
    
    def disconnect(self) -> None:
        ...
    
    @syncmethod
    def connect_sessionless(self) -> None:
        ...
    
    def disconnect_sessionless(self) -> None:
        ...
    
    @syncmethod
    def connect_socket(self) -> None:
        ...
    
    def disconnect_socket(self) -> None:
        ...
    
    def set_user(self, username: str) -> None:
        ...
    
    def set_password(self, pwd: str) -> None:
        ...
    
    def set_locale(self, locale: Sequence[str]) -> None:
        ...
    
    @syncmethod
    def load_private_key(self, path: str, password: Optional[Union[str, bytes]] = ..., extension: Optional[str] = ...) -> None:
        ...
    
    @syncmethod
    def load_client_certificate(self, path: str, extension: Optional[str] = ...) -> None:
        ...
    
    @syncmethod
    def load_type_definitions(self, nodes: list[SyncNode] | None = None) -> None:
        ...
    
    @syncmethod
    def load_data_type_definitions(self, node: Optional[SyncNode] = ..., overwrite_existing: bool = ...) -> Dict[str, Type]:
        ...
    
    @syncmethod
    def get_namespace_array(self) -> List[str]:
        ...
    
    @syncmethod
    def set_security(self) -> None:
        ...
    
    @syncmethod
    def set_security_string(self, string: str) -> None:
        ...
    
    @syncmethod
    def load_enums(self) -> Dict[str, Type]:
        ...
    
    def create_subscription(self, period: Union[ua.CreateSubscriptionParameters, float], handler: subscription.SubscriptionHandler, publishing: bool = ...) -> Subscription:
        ...
    
    def get_subscription_revised_params(self, params: ua.CreateSubscriptionParameters, results: ua.CreateSubscriptionResult) -> Optional[ua.ModifySubscriptionParameters]:
        ...
    
    @syncmethod
    def delete_subscriptions(self, subscription_ids: Iterable[int]) -> List[ua.StatusCode]:
        ...
    
    @syncmethod
    def get_namespace_index(self, uri: str) -> int:
        ...
    
    def get_node(self, nodeid: Union[SyncNode, ua.NodeId, str, int]) -> SyncNode:
        ...
    
    def get_root_node(self) -> SyncNode:
        ...
    
    def get_objects_node(self) -> SyncNode:
        ...
    
    def get_server_node(self) -> SyncNode:
        ...
    
    @syncmethod
    def connect_and_get_server_endpoints(self) -> List[ua.EndpointDescription]:
        ...
    
    @syncmethod
    def connect_and_find_servers(self) -> List[ua.ApplicationDescription]:
        ...
    
    @syncmethod
    def connect_and_find_servers_on_network(self) -> List[ua.FindServersOnNetworkResult]:
        ...
    
    @syncmethod
    def send_hello(self) -> None:
        ...
    
    @syncmethod
    def open_secure_channel(self, renew=...) -> None:
        ...
    
    @syncmethod
    def close_secure_channel(self) -> None:
        ...
    
    @syncmethod
    def get_endpoints(self) -> List[ua.EndpointDescription]:
        ...
    
    @syncmethod
    def register_server(self, server: Server, discovery_configuration: Optional[ua.DiscoveryConfiguration] = ...) -> None:
        ...
    
    @syncmethod
    def unregister_server(self, server: Server, discovery_configuration: Optional[ua.DiscoveryConfiguration] = ...) -> None:
        ...
    
    @syncmethod
    def find_servers(self, uris: Optional[Iterable[str]] = ...) -> List[ua.ApplicationDescription]:
        ...
    
    @syncmethod
    def find_servers_on_network(self) -> List[ua.FindServersOnNetworkResult]:
        ...
    
    @syncmethod
    def create_session(self) -> ua.CreateSessionResult:
        ...
    
    @syncmethod
    def check_connection(self) -> None:
        ...
    
    def server_policy(self, token_type: ua.UserTokenType) -> ua.UserTokenPolicy:
        ...
    
    @syncmethod
    def activate_session(self, username: Optional[str] = ..., password: Optional[str] = ..., certificate: Optional[x509.Certificate] = ...) -> ua.ActivateSessionResult:
        ...
    
    @syncmethod
    def close_session(self) -> None:
        ...
    
    def get_keepalive_count(self, period: float) -> int:
        ...
    
    @syncmethod
    def delete_nodes(self, nodes: Iterable[SyncNode], recursive=...) -> Tuple[List[SyncNode], List[ua.StatusCode]]:
        ...
    
    @syncmethod
    def import_xml(self, path=..., xmlstring=..., strict_mode=...) -> List[ua.NodeId]:
        ...
    
    @syncmethod
    def export_xml(self, nodes, path, export_values: bool = ...) -> None:
        ...
    
    @syncmethod
    def register_namespace(self, uri: str) -> int:
        ...
    
    @syncmethod
    def register_nodes(self, nodes: Iterable[SyncNode]) -> List[SyncNode]:
        ...
    
    @syncmethod
    def unregister_nodes(self, nodes: Iterable[SyncNode]): # -> None:
        ...
    
    @syncmethod
    def read_attributes(self, nodes: Iterable[SyncNode], attr: ua.AttributeIds = ...) -> List[ua.DataValue]:
        ...
    
    @syncmethod
    def read_values(self, nodes: Iterable[SyncNode]) -> List[Any]:
        ...
    
    @syncmethod
    def write_values(self, nodes: Iterable[SyncNode], values: Iterable[Any], raise_on_partial_error: bool = ...) -> List[ua.StatusCode]:
        ...
    
    @syncmethod
    def browse_nodes(self, nodes: Iterable[SyncNode]) -> List[Tuple[SyncNode, ua.BrowseResult]]:
        ...
    
    @syncmethod
    def translate_browsepaths(self, starting_node: ua.NodeId, relative_paths: Iterable[Union[ua.RelativePath, str]]) -> List[ua.BrowsePathResult]:
        ...
    
    def __enter__(self): # -> Self:
        ...
    
    def __exit__(self, exc_type, exc_value, traceback): # -> None:
        ...
    


class Shortcuts:
    root: SyncNode
    objects: SyncNode
    server: SyncNode
    base_object_type: SyncNode
    base_data_type: SyncNode
    base_event_type: SyncNode
    base_variable_type: SyncNode
    folder_type: SyncNode
    enum_data_type: SyncNode
    option_set_type: SyncNode
    types: SyncNode
    data_types: SyncNode
    event_types: SyncNode
    reference_types: SyncNode
    variable_types: SyncNode
    object_types: SyncNode
    namespace_array: SyncNode
    namespaces: SyncNode
    opc_binary: SyncNode
    base_structure_type: SyncNode
    base_union_type: SyncNode
    server_state: SyncNode
    service_level: SyncNode
    HasComponent: SyncNode
    HasProperty: SyncNode
    Organizes: SyncNode
    HasEncoding: SyncNode
    def __init__(self, tloop, aio_server) -> None:
        ...
    


class Server:
    """
    Sync Server, see doc for async Server
    the sync server has one extra parameter: sync_wrapper_timeout.
    if no ThreadLoop is provided this timeout is used to define how long the sync wrapper
    waits for an async call to return. defualt is 120s and hopefully should fit most applications
    """
    def __init__(self, shelf_file: Optional[Path] = ..., tloop=..., sync_wrapper_timeout: Optional[float] = ...) -> None:
        ...
    
    def __str__(self) -> str:
        ...
    
    __repr__ = ...
    def __enter__(self): # -> Self:
        ...
    
    def __exit__(self, exc_type, exc_value, traceback): # -> None:
        ...
    
    @syncmethod
    def load_certificate(self, path: str, format: str = ...): # -> None:
        ...
    
    @syncmethod
    def load_private_key(self, path, password=..., format=...): # -> None:
        ...
    
    def set_endpoint(self, url): # -> None:
        ...
    
    def set_server_name(self, name): # -> None:
        ...
    
    def set_security_policy(self, security_policy, permission_ruleset=...): # -> None:
        ...
    
    def set_security_IDs(self, policy_ids): # -> None:
        ...
    
    def set_identity_tokens(self, tokens): # -> None:
        ...
    
    def disable_clock(self, val: bool = ...): # -> None:
        ...
    
    @syncmethod
    def register_namespace(self, url): # -> None:
        ...
    
    @syncmethod
    def get_namespace_array(self): # -> None:
        ...
    
    @syncmethod
    def start(self): # -> None:
        ...
    
    def stop(self): # -> None:
        ...
    
    def link_method(self, node, callback): # -> None:
        ...
    
    @syncmethod
    def get_event_generator(self, etype=..., emitting_node=...): # -> None:
        ...
    
    def get_node(self, nodeid): # -> SyncNode:
        ...
    
    @syncmethod
    def import_xml(self, path=..., xmlstring=..., strict_mode=...): # -> None:
        ...
    
    @syncmethod
    def get_namespace_index(self, url): # -> None:
        ...
    
    @syncmethod
    def load_enums(self): # -> None:
        ...
    
    @syncmethod
    def load_type_definitions(self): # -> None:
        ...
    
    @syncmethod
    def load_data_type_definitions(self, node=...): # -> None:
        ...
    
    @syncmethod
    def write_attribute_value(self, nodeid, datavalue, attr=...): # -> None:
        ...
    
    def set_attribute_value_callback(self, nodeid: ua.NodeId, callback: Callable[[ua.NodeId, ua.AttributeIds], ua.DataValue], attr=...) -> None:
        ...
    
    def create_subscription(self, period, handler): # -> Subscription:
        ...
    


class EventGenerator:
    def __init__(self, tloop, aio_evgen) -> None:
        ...
    
    @property
    def event(self):
        ...
    
    def trigger(self, time=..., message=...):
        ...
    


def new_node(sync_node, nodeid): # -> SyncNode:
    """
    given a sync node, create a new SyncNode with the given nodeid
    """
    ...

class SyncNode:
    def __init__(self, tloop: ThreadLoop, aio_node: node.Node) -> None:
        ...
    
    def __eq__(self, other) -> bool:
        ...
    
    def __ne__(self, other) -> bool:
        ...
    
    def __str__(self) -> str:
        ...
    
    def __repr__(self): # -> str:
        ...
    
    def __hash__(self) -> int:
        ...
    
    nodeid: ua.NodeId = ...
    @syncmethod
    def read_type_definition(self) -> Optional[ua.NodeId]:
        ...
    
    @syncmethod
    def get_parent(self) -> Optional[SyncNode]:
        ...
    
    @syncmethod
    def read_node_class(self) -> ua.NodeClass:
        ...
    
    @syncmethod
    def read_attribute(self, attr: ua.AttributeIds, indexrange: Optional[str] = ..., raise_on_bad_status: bool = ...) -> ua.DataValue:
        ...
    
    @syncmethod
    def write_attribute(self, attributeid: ua.AttributeIds, datavalue: ua.DataValue, indexrange: Optional[str] = ...) -> None:
        ...
    
    @syncmethod
    def read_browse_name(self) -> ua.QualifiedName:
        ...
    
    @syncmethod
    def read_display_name(self) -> ua.LocalizedText:
        ...
    
    @syncmethod
    def read_data_type(self) -> ua.NodeId:
        ...
    
    @syncmethod
    def read_array_dimensions(self) -> List[int]:
        ...
    
    @syncmethod
    def read_value_rank(self) -> int:
        ...
    
    @syncmethod
    def delete(self, delete_references: bool = ..., recursive: bool = ...) -> List[SyncNode]:
        ...
    
    @syncmethod
    def get_children(self, refs: int = ..., nodeclassmask: ua.NodeClass = ...) -> List[SyncNode]:
        ...
    
    @syncmethod
    def get_properties(self) -> List[SyncNode]:
        ...
    
    @syncmethod
    def get_children_descriptions(self, refs: int = ..., nodeclassmask: ua.NodeClass = ..., includesubtypes: bool = ..., result_mask: ua.BrowseResultMask = ...) -> List[ua.ReferenceDescription]:
        ...
    
    @syncmethod
    def get_user_access_level(self) -> Set[ua.AccessLevel]:
        ...
    
    @overload
    def get_child(self, path: Union[ua.QualifiedName, str, Iterable[Union[ua.QualifiedName, str]]], return_all: Literal[False] = ...) -> SyncNode:
        ...
    
    @overload
    def get_child(self, path: Union[ua.QualifiedName, str, Iterable[Union[ua.QualifiedName, str]]], return_all: Literal[True] = ...) -> List[SyncNode]:
        ...
    
    @syncmethod
    def get_child(self, path: Union[ua.QualifiedName, str, Iterable[Union[ua.QualifiedName, str]]], return_all: bool = ...) -> Union[SyncNode, List[SyncNode]]:
        ...
    
    @syncmethod
    def get_children_by_path(self, paths: Iterable[Union[ua.QualifiedName, str, Iterable[Union[ua.QualifiedName, str]]]], raise_on_partial_error: bool = ...) -> List[List[Optional[SyncNode]]]:
        ...
    
    @syncmethod
    def read_raw_history(self, starttime: Optional[datetime] = ..., endtime: Optional[datetime] = ..., numvalues: int = ..., return_bounds: bool = ...) -> List[ua.DataValue]:
        ...
    
    @syncmethod
    def history_read(self, details: ua.ReadRawModifiedDetails, continuation_point: Optional[bytes] = ...) -> ua.HistoryReadResult:
        ...
    
    @syncmethod
    def read_event_history(self, starttime: datetime = ..., endtime: datetime = ..., numvalues: int = ..., evtypes: Union[SyncNode, ua.NodeId, str, int, Iterable[Union[SyncNode, ua.NodeId, str, int]]] = ...) -> List[Event]:
        ...
    
    @syncmethod
    def history_read_events(self, details: Iterable[ua.ReadEventDetails]) -> ua.HistoryReadResult:
        ...
    
    @syncmethod
    def set_modelling_rule(self, mandatory: bool) -> None:
        ...
    
    @syncmethod
    def add_variable(self, nodeid: Union[ua.NodeId, str], bname: Union[ua.QualifiedName, str], val: Any, varianttype: Optional[ua.VariantType] = ..., datatype: Optional[Union[ua.NodeId, int]] = ...) -> SyncNode:
        ...
    
    @syncmethod
    def add_property(self, nodeid: Union[ua.NodeId, str], bname: Union[ua.QualifiedName, str], val: Any, varianttype: Optional[ua.VariantType] = ..., datatype: Optional[Union[ua.NodeId, int]] = ...) -> SyncNode:
        ...
    
    @syncmethod
    def add_object(self, nodeid: Union[ua.NodeId, str], bname: Union[ua.QualifiedName, str], objecttype: Optional[int] = ..., instantiate_optional: bool = ...) -> SyncNode:
        ...
    
    @syncmethod
    def add_object_type(self, nodeid: Union[ua.NodeId, str], bname: Union[ua.QualifiedName, str]) -> SyncNode:
        ...
    
    @syncmethod
    def add_variable_type(self, nodeid: Union[ua.NodeId, str], bname: Union[ua.QualifiedName, str], datatype: Union[ua.NodeId, int]) -> SyncNode:
        ...
    
    @syncmethod
    def add_folder(self, nodeid: Union[ua.NodeId, str], bname: Union[ua.QualifiedName, str]) -> SyncNode:
        ...
    
    @syncmethod
    def add_method(self, *args) -> SyncNode:
        ...
    
    @syncmethod
    def add_data_type(self, nodeid: Union[ua.NodeId, str], bname: Union[ua.QualifiedName, str], description: Optional[str] = ...) -> SyncNode:
        ...
    
    @syncmethod
    def set_writable(self, writable: bool = ...) -> None:
        ...
    
    @syncmethod
    def write_value(self, value: Any, varianttype: Optional[ua.VariantType] = ...) -> None:
        ...
    
    set_value = ...
    @syncmethod
    def write_params(self, params: ua.WriteParameters) -> List[ua.StatusCode]:
        ...
    
    @syncmethod
    def read_params(self, params: ua.ReadParameters) -> List[ua.DataValue]:
        ...
    
    @syncmethod
    def read_value(self) -> Any:
        ...
    
    get_value = ...
    @syncmethod
    def read_data_value(self, raise_on_bad_status: bool = ...) -> ua.DataValue:
        ...
    
    get_data_value = ...
    @syncmethod
    def read_data_type_as_variant_type(self) -> ua.VariantType:
        ...
    
    get_data_type_as_variant_type = ...
    @syncmethod
    def call_method(self, methodid: Union[ua.NodeId, ua.QualifiedName, str], *args) -> Any:
        ...
    
    @syncmethod
    def get_references(self, refs: int = ..., direction: ua.BrowseDirection = ..., nodeclassmask: ua.NodeClass = ..., includesubtypes: bool = ..., result_mask: ua.BrowseResultMask = ...) -> List[ua.ReferenceDescription]:
        ...
    
    @syncmethod
    def add_reference(self, target: Union[SyncNode, ua.NodeId, str, int], reftype: int, forward: bool = ..., bidirectional: bool = ...) -> None:
        ...
    
    @syncmethod
    def read_description(self) -> ua.LocalizedText:
        ...
    
    @syncmethod
    def get_variables(self) -> List[SyncNode]:
        ...
    
    @overload
    def get_path(self, max_length: int = ..., as_string: Literal[False] = ...) -> List[SyncNode]:
        ...
    
    @overload
    def get_path(self, max_length: int = ..., as_string: Literal[True] = ...) -> List[str]:
        ...
    
    @syncmethod
    def get_path(self, max_length: int = ..., as_string: bool = ...) -> Union[List[SyncNode], List[str]]:
        ...
    
    @syncmethod
    def read_attributes(self, attrs: Iterable[ua.AttributeIds]) -> List[ua.DataValue]:
        ...
    
    @syncmethod
    def add_reference_type(self, nodeid: Union[ua.NodeId, str], bname: Union[ua.QualifiedName, str], symmetric: bool = ..., inversename: Optional[str] = ...) -> SyncNode:
        ...
    
    @syncmethod
    def delete_reference(self, target: Union[SyncNode, ua.NodeId, str, int], reftype: int, forward: bool = ..., bidirectional: bool = ...) -> None:
        ...
    
    @syncmethod
    def get_access_level(self) -> Set[ua.AccessLevel]:
        ...
    
    @syncmethod
    def get_description_refs(self) -> List[SyncNode]:
        ...
    
    @syncmethod
    def get_encoding_refs(self) -> List[SyncNode]:
        ...
    
    @syncmethod
    def get_methods(self) -> List[SyncNode]:
        ...
    
    @syncmethod
    def get_referenced_nodes(self, refs: int = ..., direction: ua.BrowseDirection = ..., nodeclassmask: ua.NodeClass = ..., includesubtypes: bool = ...) -> List[SyncNode]:
        ...
    
    @syncmethod
    def read_data_type_definition(self) -> ua.DataTypeDefinition:
        ...
    
    @syncmethod
    def read_event_notifier(self) -> Set[ua.EventNotifier]:
        ...
    
    @syncmethod
    def register(self) -> None:
        ...
    
    @syncmethod
    def set_attr_bit(self, attr: ua.AttributeIds, bit: int) -> None:
        ...
    
    @syncmethod
    def set_event_notifier(self, values) -> None:
        ...
    
    @syncmethod
    def unregister(self) -> None:
        ...
    
    @syncmethod
    def unset_attr_bit(self, attr: ua.AttributeIds, bit: int) -> None:
        ...
    
    @syncmethod
    def write_array_dimensions(self, value: int) -> None:
        ...
    
    @syncmethod
    def write_data_type_definition(self, sdef: ua.DataTypeDefinition) -> None:
        ...
    
    @syncmethod
    def write_value_rank(self, value: int) -> None:
        ...
    


class Subscription:
    def __init__(self, tloop, sub) -> None:
        ...
    
    @syncmethod
    def subscribe_data_change(self, nodes, attr=..., queuesize=..., monitoring=..., sampling_interval=...): # -> None:
        ...
    
    @syncmethod
    def subscribe_events(self, sourcenode=..., evtypes=..., evfilter=..., queuesize=...): # -> None:
        ...
    
    @syncmethod
    def unsubscribe(self, handle): # -> None:
        ...
    
    @syncmethod
    def create_monitored_items(self, monitored_items): # -> None:
        ...
    
    @syncmethod
    def delete(self): # -> None:
        ...
    


class XmlExporter:
    def __init__(self, sync_server) -> None:
        ...
    
    @syncmethod
    def build_etree(self, node_list, uris=...): # -> None:
        ...
    
    @syncmethod
    def write_xml(self, xmlpath: Path, pretty=...): # -> None:
        ...
    


class DataTypeDictionaryBuilder:
    def __init__(self, server, idx, ns_urn, dict_name, dict_node_id=...) -> None:
        ...
    
    @property
    def dict_id(self): # -> None:
        ...
    
    @syncmethod
    def init(self): # -> None:
        ...
    
    @syncmethod
    def create_data_type(self, type_name, nodeid=..., init=...): # -> None:
        ...
    
    @syncmethod
    def set_dict_byte_string(self): # -> None:
        ...
    


def new_struct_field(name: str, dtype: Union[ua.NodeId, SyncNode, ua.VariantType], array: bool = ..., optional: bool = ..., description: str = ...) -> ua.StructureField:
    ...

@syncfunc(aio_func=common.structures104.new_enum)
def new_enum(server: Union[Server, Client], idx: Union[int, ua.NodeId], name: Union[int, ua.QualifiedName], values: List[str], optional: bool = ...) -> SyncNode: ...

@syncfunc(aio_func=common.structures104.new_struct)
def new_struct(server: Union[Server, Client], idx: Union[int, ua.NodeId], name: Union[int, ua.QualifiedName], fields: List[ua.StructureField]) -> Tuple[SyncNode, List[SyncNode]]: ...

