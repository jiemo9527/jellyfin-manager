from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.getenv("JM_HOST", "0.0.0.0")
    port = int(os.getenv("JM_PORT", "18080"))
    uvicorn.run("jm_manager.app:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
