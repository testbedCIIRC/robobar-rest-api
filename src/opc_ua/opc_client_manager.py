"""Manager for OPC UA client instance."""

from threading import Thread

from pydantic import BaseModel

from opc_ua.opc_client import RobobarOpcClient


class OpcClientManager(BaseModel):
    """Manager for OPC UA client instance."""

    model_config = {
        "arbitrary_types_allowed": True,
    }
    client_instance: RobobarOpcClient | None = None
    connection_thread: Thread | None = None

    def create_new_instance(self, url: str) -> None:
        """Create a new OPC UA client instance."""
        self.client_instance = RobobarOpcClient(url)

    def start_connection_thread(self) -> None:
        """Start the connection maintenance thread."""
        if self.client_instance is None:
            error_msg = "Client instance is not created."
            raise ValueError(error_msg)

        self.connection_thread = Thread(
            target=self.client_instance.create_and_maintain_connection,
        )
        self.connection_thread.daemon = True
        self.connection_thread.start()


opc_manager_instance: OpcClientManager = OpcClientManager()
