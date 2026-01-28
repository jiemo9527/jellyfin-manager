from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class JellyfinApi:
    base_url: str
    api_key: str

    def headers(self) -> dict[str, str]:
        return {"X-Emby-Token": self.api_key, "Content-Type": "application/json"}

    def get_users(self) -> list[dict[str, Any]]:
        r = requests.get(f"{self.base_url}/Users", headers=self.headers(), timeout=15)
        r.raise_for_status()
        return r.json()

    def create_user(self, username: str, password: str) -> dict[str, Any]:
        r = requests.post(
            f"{self.base_url}/Users/New",
            json={"Name": username, "Password": password},
            headers=self.headers(),
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    def get_user(self, user_id: str) -> dict[str, Any]:
        r = requests.get(f"{self.base_url}/Users/{user_id}", headers=self.headers(), timeout=15)
        r.raise_for_status()
        return r.json()

    def delete_user(self, user_id: str) -> None:
        r = requests.delete(f"{self.base_url}/Users/{user_id}", headers=self.headers(), timeout=15)
        if r.status_code in (204, 404):
            return
        if r.status_code != 204:
            r.raise_for_status()

    def update_policy(self, user_id: str, policy: dict[str, Any]) -> None:
        r = requests.post(f"{self.base_url}/Users/{user_id}/Policy", json=policy, headers=self.headers(), timeout=15)
        if r.status_code != 204:
            r.raise_for_status()

    def set_disabled(self, user_id: str, disabled: bool) -> None:
        user = self.get_user(user_id)
        policy = user.get("Policy", {})
        policy["IsDisabled"] = bool(disabled)
        self.update_policy(user_id, policy)

    def set_initial_policy(self, user_id: str) -> None:
        user = self.get_user(user_id)
        policy = user.get("Policy", {})
        policy["EnableContentDownloading"] = False
        policy["MaxActiveSessions"] = 3
        policy["IsDisabled"] = False
        self.update_policy(user_id, policy)

    def refresh_library_default(self, library_id: str) -> None:
        url = (
            f"{self.base_url}/Items/{library_id}/Refresh"
            "?Recursive=true&ImageRefreshMode=Default&MetadataRefreshMode=Default"
            "&ReplaceAllImages=false&RegenerateTrickplay=false&ReplaceAllMetadata=false"
        )
        r = requests.post(url, headers=self.headers(), timeout=30)
        if r.status_code != 204:
            r.raise_for_status()

    def get_devices(self) -> list[dict[str, Any]]:
        r = requests.get(f"{self.base_url}/Devices", headers=self.headers(), timeout=15)
        r.raise_for_status()
        data = r.json()
        items = data.get("Items", []) if isinstance(data, dict) else []
        return items if isinstance(items, list) else []

    def delete_device(self, device_id: str) -> bool:
        r = requests.delete(f"{self.base_url}/Devices?Id={device_id}", headers=self.headers(), timeout=15)
        if r.status_code in (200, 204, 404):
            return True
        r.raise_for_status()
        return False
