"""Main entry point for the Robobar REST API server."""

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from opc_ua.opc_client_manager import opc_manager_instance
from routes.root import root_router
from utilities.logger_functions import logger


def main() -> None:
    """Serve as the main entry point for the Robobar REST API server."""
    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(root_router)

    opc_manager_instance.create_new_instance("opc.tpc://10.100.0.210:4840")
    if opc_manager_instance.client_instance is None:
        return
    opc_manager_instance.start_connection_thread()

    uvicorn.run(app, host="127.0.0.1", port=8000)

    opc_manager_instance.client_instance.exit = True
    if opc_manager_instance.connection_thread is not None:
        logger.info("Waiting for connection thread to finish...")
        opc_manager_instance.connection_thread.join()


if __name__ == "__main__":
    main()
